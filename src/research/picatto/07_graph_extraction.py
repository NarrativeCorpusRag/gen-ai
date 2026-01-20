import multiprocessing as mp
import polars as pl
from pathlib import Path
from typing import List, Dict, Tuple
from tqdm import tqdm
import time
import json
import gc
from relik import Relik
import fsspec
import hashlib

INPUT_PATH = "gs://gen-ai-tu/news/chunks/"
OUTPUT_PATH = "gs://gen-ai-tu/news/graph_rag/"
CHECKPOINT_FILE = Path(OUTPUT_PATH) / ".checkpoint.json"
LOCAL_CHECKPOINT = "/home/user/hernan/graph_rag_checkpoint.json"

NUM_GPUS = 2
BATCH_SIZE = 256  # Can be higher with small models
CONFIDENCE_THRESHOLD = 0.5

# Both models
ENTITY_MODEL = "sapienzanlp/relik-entity-linking-small"
RELATION_MODEL = "sapienzanlp/relik-relation-extraction-nyt-large"

def list_gcs_parquet_files(gcs_path: str) -> List[str]:
    """List all parquet files in GCS path"""
    fs = fsspec.filesystem('gcs')
    bucket_path = gcs_path.replace("gs://", "")
    all_files = fs.glob(f"{bucket_path}**/*.parquet")
    return [f"gs://{f}" for f in all_files]


def read_parquet_gcs(gcs_path: str) -> pl.DataFrame:
    """Read parquet from GCS"""
    return pl.read_parquet(gcs_path)


def write_parquet_gcs(df: pl.DataFrame, gcs_path: str):
    """Write parquet to GCS"""
    df.write_parquet(gcs_path, compression='zstd')
    
def process_dual_model_output(
    entity_response,
    relation_response,
    chunk_id: str,
    confidence_threshold: float
) -> Tuple[List[Dict], List[Dict]]:
    """
    Combine outputs from entity linking and relation extraction models.
    
    - entity_response: from relik-entity-linking (has Wikipedia IDs)
    - relation_response: from relik-relation-extraction (has triplets)
    """
    
    entities = []
    entity_text_to_id = {}
    entity_text_to_wikipedia = {}
    
    # --- ENTITIES (from entity linking model) ---
    if entity_response and hasattr(entity_response, 'spans') and entity_response.spans:
        for span in entity_response.spans:
            ent_id = hashlib.sha256(f"{chunk_id}:{span.text}:{span.start}".encode()).hexdigest()[:16]
            entity_text_to_id[span.text] = ent_id
            
            # Get Wikipedia ID (entity linking provides this)
            wikipedia_id = None
            if hasattr(span, 'id') and span.id:
                wikipedia_id = span.id
                entity_text_to_wikipedia[span.text] = wikipedia_id
            
            ent = {
                "entity_id": ent_id,
                "chunk_id": chunk_id,
                "text": span.text,
                "label": getattr(span, 'label', 'ENTITY'),
                "start": span.start,
                "end": span.end,
                "wikipedia_id": wikipedia_id,
            }
            entities.append(ent)
    
    # --- RELATIONS (from relation extraction model) ---
    relations = []
    if relation_response and hasattr(relation_response, 'triplets') and relation_response.triplets:
        for triplet in relation_response.triplets:
            conf = getattr(triplet, 'confidence', 1.0)
            if conf >= confidence_threshold:
                head_text = triplet.subject.text
                tail_text = triplet.object.text
                rel_id = hashlib.sha256(
                    f"{chunk_id}:{head_text}:{triplet.label}:{tail_text}".encode()
                ).hexdigest()[:16]
                
                # Link to entities from entity linking model
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
    file_queue: mp.Queue,
    result_queue: mp.Queue,
    batch_size: int,
    output_path: str,
    entity_model_name: str,
    relation_model_name: str,
    confidence_threshold: float
):
    """Worker process with both entity linking and relation extraction models"""
    import torch
    from relik import Relik
    
    device = f"cuda:{gpu_id}"
    
    # Load BOTH models on same GPU
    print(f"[GPU {gpu_id}] Loading entity model: {entity_model_name}")
    entity_model = Relik.from_pretrained(entity_model_name, device=device)
    
    print(f"[GPU {gpu_id}] Loading relation model: {relation_model_name}")
    relation_model = Relik.from_pretrained(relation_model_name, device=device)
    
    print(f"[GPU {gpu_id}] Both models loaded!")
    
    while True:
        item = file_queue.get()
        if item is None:
            break
        
        file_idx, parquet_file = item
        
        try:
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
                
                # Filter empty
                valid_mask = [bool(t and len(str(t).strip()) > 10) for t in texts]
                valid_texts = [t for t, v in zip(texts, valid_mask) if v]
                
                # Initialize predictions
                entity_preds = [None] * len(texts)
                relation_preds = [None] * len(texts)
                
                if valid_texts:
                    # --- ENTITY LINKING ---
                    try:
                        ent_results = entity_model(valid_texts)
                        if not isinstance(ent_results, list):
                            ent_results = [ent_results]
                        
                        valid_idx = 0
                        for i, v in enumerate(valid_mask):
                            if v:
                                entity_preds[i] = ent_results[valid_idx]
                                valid_idx += 1
                    except Exception as e:
                        print(f"[GPU {gpu_id}] Entity model error: {e}")
                    
                    # --- RELATION EXTRACTION ---
                    try:
                        rel_results = relation_model(valid_texts)
                        if not isinstance(rel_results, list):
                            rel_results = [rel_results]
                        
                        valid_idx = 0
                        for i, v in enumerate(valid_mask):
                            if v:
                                relation_preds[i] = rel_results[valid_idx]
                                valid_idx += 1
                    except Exception as e:
                        print(f"[GPU {gpu_id}] Relation model error: {e}")
                
                # Process results
                chunk_ids = batch_df['chunk_id'].to_list()
                doc_ids = batch_df['doc_id'].to_list()
                chunk_indices = batch_df['chunk_index'].to_list()
                
                meta_cols = ['uri', 'host', 'year', 'month']
                meta_data = {col: batch_df[col].to_list() for col in meta_cols if col in batch_df.columns}
                
                for i in range(len(texts)):
                    chunk_id = chunk_ids[i]
                    
                    # Combine outputs from both models
                    entities, relations = process_dual_model_output(
                        entity_preds[i],
                        relation_preds[i],
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
            
            # Write outputs to GCS
            if all_chunks:
                chunks_df = pl.DataFrame(all_chunks)
                chunks_df.write_parquet(
                    f"{output_path}chunks_graph/chunks_{file_idx:05d}.parquet",
                    compression='zstd'
                )
                del chunks_df
            
            if all_entities:
                entities_df = pl.DataFrame(all_entities)
                entities_df.write_parquet(
                    f"{output_path}entities/entities_{file_idx:05d}.parquet",
                    compression='zstd'
                )
                del entities_df
            
            if all_relations:
                relations_df = pl.DataFrame(all_relations)
                relations_df.write_parquet(
                    f"{output_path}relations/relations_{file_idx:05d}.parquet",
                    compression='zstd'
                )
                del relations_df
            
            result_queue.put((file_idx, len(all_chunks), len(all_entities), len(all_relations), True))
            
            del df, all_chunks, all_entities, all_relations
            gc.collect()
            torch.cuda.empty_cache()
            
        except Exception as e:
            print(f"[GPU {gpu_id}] Error on {parquet_file}: {e}")
            import traceback
            traceback.print_exc()
            result_queue.put((file_idx, 0, 0, 0, False))
            
def load_checkpoint():
    if Path(LOCAL_CHECKPOINT).exists():
        with open(LOCAL_CHECKPOINT) as f:
            data = json.load(f)
            return (
                set(data.get('completed_files', [])),
                data.get('total_chunks', 0),
                data.get('total_entities', 0),
                data.get('total_relations', 0)
            )
    return set(), 0, 0, 0

def save_checkpoint(completed_files, total_chunks, total_entities, total_relations):
    with open(LOCAL_CHECKPOINT, 'w') as f:
        json.dump({
            'completed_files': list(completed_files),
            'total_chunks': total_chunks,
            'total_entities': total_entities,
            'total_relations': total_relations,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }, f)
        
def process_multi_gpu(
    input_path: str,
    output_path: str,
    num_gpus: int = 2,
    batch_size: int = 256,
    entity_model: str = ENTITY_MODEL,
    relation_model: str = RELATION_MODEL,
    confidence_threshold: float = CONFIDENCE_THRESHOLD
):
    """Process with multiple GPUs, each running both models"""
    
    mp.set_start_method('spawn', force=True)
    
    print(f"Entity model: {entity_model}")
    print(f"Relation model: {relation_model}")
    
    print(f"Listing files from {input_path}...")
    input_files = list_gcs_parquet_files(input_path)
    input_files = sorted(input_files)
    print(f"Found {len(input_files)} files")
    
    if not input_files:
        print("No files found!")
        return
    
    completed_files, total_chunks, total_entities, total_relations = load_checkpoint()
    files_to_process = [
        (i, f) for i, f in enumerate(input_files)
        if i not in completed_files
    ]
    print(f"Files to process: {len(files_to_process)} / {len(input_files)}")
    
    if not files_to_process:
        print("All files already processed!")
        return
    
    file_queue = mp.Queue()
    result_queue = mp.Queue()
    
    # Start GPU workers
    workers = []
    for gpu_id in range(num_gpus):
        p = mp.Process(
            target=gpu_worker,
            args=(
                gpu_id,
                file_queue,
                result_queue,
                batch_size,
                output_path,
                entity_model,
                relation_model,
                confidence_threshold
            )
        )
        p.start()
        workers.append(p)
        print(f"Started worker for GPU {gpu_id}")
    
    # Feed files
    for file_idx, parquet_file in files_to_process:
        file_queue.put((file_idx, parquet_file))
    
    for _ in workers:
        file_queue.put(None)
    
    # Collect results
    start_time = time.time()
    pbar = tqdm(total=len(files_to_process), desc="Processing")
    
    results_received = 0
    while results_received < len(files_to_process):
        file_idx, chunks, entities, relations, success = result_queue.get()
        
        if success:
            completed_files.add(file_idx)
            total_chunks += chunks
            total_entities += entities
            total_relations += relations
        
        results_received += 1
        pbar.update(1)
        
        elapsed = time.time() - start_time
        chunks_per_sec = total_chunks / elapsed if elapsed > 0 else 0
        
        pbar.set_postfix({
            'chunks': f'{total_chunks:,}',
            'ent': f'{total_entities:,}',
            'rel': f'{total_relations:,}',
            'c/s': f'{chunks_per_sec:.1f}'
        })
        
        if results_received % 20 == 0:
            save_checkpoint(completed_files, total_chunks, total_entities, total_relations)
    
    pbar.close()
    
    for p in workers:
        p.join()
    
    save_checkpoint(completed_files, total_chunks, total_entities, total_relations)
    
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"COMPLETE!")
    print(f"Chunks: {total_chunks:,}")
    print(f"Entities: {total_entities:,}")
    print(f"Relations: {total_relations:,}")
    print(f"Time: {elapsed/3600:.2f} hours")
    print(f"Speed: {total_chunks/elapsed:.1f} chunks/sec")
    print(f"{'='*60}")

if __name__ == "__main__":
    process_multi_gpu(
        INPUT_PATH,
        OUTPUT_PATH,
        num_gpus=NUM_GPUS,
        batch_size=BATCH_SIZE
    )