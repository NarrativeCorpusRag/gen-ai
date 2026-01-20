# worker.py
import polars as pl
import hashlib
import gc
from typing import List, Dict, Tuple


def process_joint_output(
    response,
    chunk_id: str,
    confidence_threshold: float
) -> Tuple[List[Dict], List[Dict]]:
    """Process output from joint CIE model (has both spans AND triplets)"""
    
    entities = []
    entity_text_to_id = {}
    entity_text_to_wikipedia = {}
    
    # Entities with Wikipedia IDs
    if response and hasattr(response, 'spans') and response.spans:
        for span in response.spans:
            ent_id = hashlib.sha256(f"{chunk_id}:{span.text}:{span.start}".encode()).hexdigest()[:16]
            entity_text_to_id[span.text] = ent_id
            
            wikipedia_id = getattr(span, 'id', None)
            if wikipedia_id:
                entity_text_to_wikipedia[span.text] = wikipedia_id
            
            entities.append({
                "entity_id": ent_id,
                "chunk_id": chunk_id,
                "text": span.text,
                "label": getattr(span, 'label', 'ENTITY'),
                "start": span.start,
                "end": span.end,
                "wikipedia_id": wikipedia_id,
            })
    
    # Relations
    relations = []
    if response and hasattr(response, 'triplets') and response.triplets:
        for triplet in response.triplets:
            conf = getattr(triplet, 'confidence', 1.0)
            if conf >= confidence_threshold:
                head_text = triplet.subject.text
                tail_text = triplet.object.text
                rel_id = hashlib.sha256(
                    f"{chunk_id}:{head_text}:{triplet.label}:{tail_text}".encode()
                ).hexdigest()[:16]
                
                relations.append({
                    "relation_id": rel_id,
                    "chunk_id": chunk_id,
                    "head_text": head_text,
                    "head_id": entity_text_to_id.get(head_text),
                    "head_wikipedia_id": entity_text_to_wikipedia.get(head_text),
                    "relation": triplet.label,
                    "tail_text": tail_text,
                    "tail_id": entity_text_to_id.get(tail_text),
                    "tail_wikipedia_id": entity_text_to_wikipedia.get(tail_text),
                    "confidence": float(conf)
                })
    
    return entities, relations


def gpu_worker(
    gpu_id: int,
    file_queue,
    result_queue,
    batch_size: int,
    output_path: str,
    model_name: str,
    confidence_threshold: float
):
    """Worker with SINGLE joint model"""
    import torch
    from relik import Relik
    
    device = f"cuda:{gpu_id}"
    
    print(f"[GPU {gpu_id}] Loading joint model: {model_name}")
    model = Relik.from_pretrained(model_name, device=device)
    print(f"[GPU {gpu_id}] Model loaded!")
    
    while True:
        item = file_queue.get()
        if item is None:
            break
        
        file_idx, parquet_file = item
        
        try:
            print(f"[GPU {gpu_id}] Processing file {file_idx}: {parquet_file}")
            df = pl.read_parquet(parquet_file)
            
            if df.is_empty():
                result_queue.put((file_idx, 0, 0, 0, True))
                continue
            
            all_chunks = []
            all_entities = []
            all_relations = []
            
            for batch_start in range(0, len(df), batch_size):
                batch_df = df.slice(batch_start, batch_size)
                texts = batch_df['chunk_text'].to_list()
                
                # Filter empty/short texts
                valid_mask = [
                    bool(t and isinstance(t, str) and len(t.strip()) > 20)
                    for t in texts
                ]
                valid_texts = [t for t, v in zip(texts, valid_mask) if v]
                
                # Single model inference (instead of 2!)
                preds = [None] * len(texts)
                
                if valid_texts:
                    try:
                        results = model(valid_texts)
                        if not isinstance(results, list):
                            results = [results]
                        
                        valid_idx = 0
                        for i, v in enumerate(valid_mask):
                            if v:
                                preds[i] = results[valid_idx]
                                valid_idx += 1
                    except Exception as e:
                        print(f"[GPU {gpu_id}] Inference error: {e}")
                
                # Process results
                chunk_ids = batch_df['chunk_id'].to_list()
                doc_ids = batch_df['doc_id'].to_list()
                chunk_indices = batch_df['chunk_index'].to_list()
                
                meta_cols = ['uri', 'host', 'year', 'month']
                meta_data = {col: batch_df[col].to_list() for col in meta_cols if col in batch_df.columns}
                
                for i in range(len(texts)):
                    chunk_id = chunk_ids[i]
                    
                    entities, relations = process_joint_output(
                        preds[i],
                        chunk_id,
                        confidence_threshold
                    )
                    
                    all_entities.extend(entities)
                    all_relations.extend(relations)
                    
                    chunk_doc = {
                        'chunk_id': chunk_id,
                        'doc_id': doc_ids[i],
                        'chunk_index': chunk_indices[i],
                        'chunk_text': texts[i],
                        'entities': entities,
                        'relations': relations,
                        'entity_count': len(entities),
                        'relation_count': len(relations),
                    }
                    for col in meta_cols:
                        if col in meta_data:
                            chunk_doc[col] = meta_data[col][i]
                    
                    all_chunks.append(chunk_doc)
                
                del batch_df
            
            # Write outputs
            print(f"[GPU {gpu_id}] File {file_idx}: {len(all_chunks)} chunks, {len(all_entities)} entities, {len(all_relations)} relations")
            
            if all_chunks:
                try:
                    chunks_df = pl.DataFrame(all_chunks)
                    out_file = f"{output_path}chunks_graph/chunks_{file_idx:05d}.parquet"
                    chunks_df.write_parquet(out_file, compression='zstd')
                    print(f"[GPU {gpu_id}] ✓ Written: {out_file}")
                except Exception as e:
                    print(f"[GPU {gpu_id}] ✗ Write failed: {e}")
                    import traceback
                    traceback.print_exc()
            
            if all_entities:
                try:
                    pl.DataFrame(all_entities).write_parquet(
                        f"{output_path}entities/entities_{file_idx:05d}.parquet",
                        compression='zstd'
                    )
                except Exception as e:
                    print(f"[GPU {gpu_id}] ✗ Entities write failed: {e}")
            
            if all_relations:
                try:
                    pl.DataFrame(all_relations).write_parquet(
                        f"{output_path}relations/relations_{file_idx:05d}.parquet",
                        compression='zstd'
                    )
                except Exception as e:
                    print(f"[GPU {gpu_id}] ✗ Relations write failed: {e}")
            
            result_queue.put((file_idx, len(all_chunks), len(all_entities), len(all_relations), True))
            
            del df, all_chunks, all_entities, all_relations
            gc.collect()
            torch.cuda.empty_cache()
            
        except Exception as e:
            print(f"[GPU {gpu_id}] Error on {parquet_file}: {e}")
            import traceback
            traceback.print_exc()
            result_queue.put((file_idx, 0, 0, 0, False))