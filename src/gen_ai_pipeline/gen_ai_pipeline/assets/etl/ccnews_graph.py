# assets/graph_extraction.py
import gc
import hashlib
import time
import torch
import torch.multiprocessing as mp
import polars as pl
import fsspec
from typing import List, Dict, Tuple
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    Config,
)
from gen_ai_pipeline.assets.etl.ccnews_chunking import ccnews_chunking_asset

class GraphExtractionConfig(Config):
    input_path: str = "gs://gen-ai-tu/news/chunks/"
    output_path: str = "gs://gen-ai-tu/news/graph_rag/"
    model_name: str = "relik-ie/relik-cie-small"
    num_gpus: int = 2
    batch_size: int = 256
    confidence_threshold: float = 0.5


def list_gcs_parquet_files(gcs_path: str) -> List[str]:
    fs = fsspec.filesystem('gcs')
    bucket_path = gcs_path.replace("gs://", "")
    all_files = fs.glob(f"{bucket_path}**/*.parquet")
    return [f"gs://{f}" for f in all_files]


def process_joint_output(
    response,
    chunk_id: str,
    confidence_threshold: float
) -> Tuple[List[Dict], List[Dict]]:
    """Process output from joint CIE model (has both spans AND triplets)"""
    entities = []
    entity_text_to_id = {}
    entity_text_to_wikipedia = {}

    if response and hasattr(response, 'spans') and response.spans:
        for span in response.spans:
            ent_id = hashlib.sha256(
                f"{chunk_id}:{span.text}:{span.start}".encode()
            ).hexdigest()[:16]
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
    file_queue: mp.Queue,
    result_queue: mp.Queue,
    batch_size: int,
    output_path: str,
    model_name: str,
    confidence_threshold: float
):
    """GPU worker process for graph extraction"""
    from relik import Relik

    device = f"cuda:{gpu_id}"
    model = Relik.from_pretrained(model_name, device=device)

    def safe_batch_inference(texts: list, valid_mask: list) -> list:
        preds = [None] * len(texts)
        valid_texts = [t for t, v in zip(texts, valid_mask) if v]

        if not valid_texts:
            return preds

        try:
            results = model(valid_texts)
            if not isinstance(results, list):
                results = [results]

            valid_idx = 0
            for i, v in enumerate(valid_mask):
                if v:
                    preds[i] = results[valid_idx]
                    valid_idx += 1
        except Exception:
            pass

        return preds

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

                valid_mask = [
                    bool(t and isinstance(t, str) and 20 < len(t.strip()) < 50000)
                    for t in texts
                ]

                preds = safe_batch_inference(texts, valid_mask)

                chunk_ids = batch_df['chunk_id'].to_list()
                doc_ids = batch_df['doc_id'].to_list()
                chunk_indices = batch_df['chunk_index'].to_list()

                meta_cols = ['uri', 'host', 'year', 'month']
                meta_data = {
                    col: batch_df[col].to_list()
                    for col in meta_cols if col in batch_df.columns
                }

                for i in range(len(texts)):
                    chunk_id = chunk_ids[i]
                    entities, relations = process_joint_output(
                        preds[i], chunk_id, confidence_threshold
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

            if all_chunks:
                pl.DataFrame(all_chunks).write_parquet(
                    f"{output_path}chunks_graph/chunks_{file_idx:05d}.parquet",
                    compression='zstd'
                )

            if all_entities:
                pl.DataFrame(all_entities).write_parquet(
                    f"{output_path}entities/entities_{file_idx:05d}.parquet",
                    compression='zstd'
                )

            if all_relations:
                pl.DataFrame(all_relations).write_parquet(
                    f"{output_path}relations/relations_{file_idx:05d}.parquet",
                    compression='zstd'
                )

            result_queue.put((file_idx, len(all_chunks), len(all_entities), len(all_relations), True))

            del df, all_chunks, all_entities, all_relations
            gc.collect()
            torch.cuda.empty_cache()

        except Exception:
            result_queue.put((file_idx, 0, 0, 0, False))


@asset(
    key_prefix=["news"],
    name="graph_extraction",
    description="Extracts entities and relations from news chunks using Relik joint CIE model",
    group_name="etl",
    compute_kind="polars",
    deps=[ccnews_chunking_asset],
)
def graph_extraction(
    context: AssetExecutionContext,
    config: GraphExtractionConfig
) -> MaterializeResult:
    mp.set_start_method('spawn', force=True)

    context.log.info(f"Model: {config.model_name}")
    context.log.info(f"Batch size: {config.batch_size}")
    context.log.info(f"GPUs: {config.num_gpus}")

    context.log.info(f"Listing files from {config.input_path}...")
    input_files = sorted(list_gcs_parquet_files(config.input_path))
    context.log.info(f"Found {len(input_files)} files")

    if not input_files:
        context.log.warning("No input files found!")
        return MaterializeResult(metadata={"status": "no_files"})

    existing_files = set()
    try:
        fs = fsspec.filesystem('gcs')
        chunks_path = config.output_path.replace("gs://", "") + "chunks_graph/"
        existing = fs.glob(f"{chunks_path}*.parquet")
        for f in existing:
            idx = int(f.split('_')[-1].replace('.parquet', ''))
            existing_files.add(idx)
        context.log.info(f"Found {len(existing_files)} already processed files")
    except Exception:
        pass

    files_to_process = [
        (i, f) for i, f in enumerate(input_files)
        if i not in existing_files
    ]
    context.log.info(f"Files to process: {len(files_to_process)} / {len(input_files)}")

    if not files_to_process:
        context.log.info("All files already processed!")
        return MaterializeResult(metadata={"status": "complete", "files_processed": 0})

    file_queue = mp.Queue()
    result_queue = mp.Queue()

    workers = []
    for gpu_id in range(config.num_gpus):
        p = mp.Process(
            target=gpu_worker,
            args=(
                gpu_id,
                file_queue,
                result_queue,
                config.batch_size,
                config.output_path,
                config.model_name,
                config.confidence_threshold
            )
        )
        p.start()
        workers.append(p)
        context.log.info(f"Started worker for GPU {gpu_id}")

    for file_idx, parquet_file in files_to_process:
        file_queue.put((file_idx, parquet_file))

    for _ in workers:
        file_queue.put(None)

    start_time = time.time()
    total_chunks = 0
    total_entities = 0
    total_relations = 0
    successful_files = 0
    failed_files = 0

    for i in range(len(files_to_process)):
        file_idx, chunks, entities, relations, success = result_queue.get()

        if success:
            total_chunks += chunks
            total_entities += entities
            total_relations += relations
            successful_files += 1
        else:
            failed_files += 1

        if (i + 1) % 10 == 0:
            elapsed = time.time() - start_time
            rate = total_chunks / elapsed if elapsed > 0 else 0
            context.log.info(
                f"Progress: {i + 1}/{len(files_to_process)} files | "
                f"{total_chunks:,} chunks | {total_entities:,} entities | "
                f"{total_relations:,} relations | {rate:.1f} chunks/sec"
            )

    for p in workers:
        p.join()

    elapsed = time.time() - start_time
    chunks_per_sec = total_chunks / elapsed if elapsed > 0 else 0

    context.log.info(f"Completed in {elapsed / 3600:.2f} hours")
    context.log.info(f"Total chunks: {total_chunks:,}")
    context.log.info(f"Total entities: {total_entities:,}")
    context.log.info(f"Total relations: {total_relations:,}")
    context.log.info(f"Speed: {chunks_per_sec:.1f} chunks/sec")

    return MaterializeResult(
        metadata={
            "files_processed": successful_files,
            "files_failed": failed_files,
            "total_chunks": total_chunks,
            "total_entities": total_entities,
            "total_relations": total_relations,
            "processing_time_hours": round(elapsed / 3600, 2),
            "chunks_per_second": round(chunks_per_sec, 1),
            "output_path": MetadataValue.path(config.output_path),
            "model": config.model_name,
        }
    )