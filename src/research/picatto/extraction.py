# run_extraction.py
import torch.multiprocessing as mp
from pathlib import Path
from tqdm import tqdm
import time
import json
import fsspec

from worker import gpu_worker

# ============================================================
# CONFIG - SINGLE JOINT MODEL
# ============================================================

INPUT_PATH = "gs://gen-ai-tu/news/chunks/"
OUTPUT_PATH = "gs://gen-ai-tu/news/graph_rag/"
LOCAL_CHECKPOINT = "./graph_rag_checkpoint.json"

NUM_GPUS = 2
BATCH_SIZE = 256 
CONFIDENCE_THRESHOLD = 0.5

# Joint model - MUCH faster
MODEL = "relik-ie/relik-cie-small"


def list_gcs_parquet_files(gcs_path: str):
    fs = fsspec.filesystem('gcs')
    bucket_path = gcs_path.replace("gs://", "")
    all_files = fs.glob(f"{bucket_path}**/*.parquet")
    return [f"gs://{f}" for f in all_files]


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


def main():
    mp.set_start_method('spawn', force=True)
    
    print(f"Model: {MODEL}")
    print(f"Batch size: {BATCH_SIZE}")
    
    print(f"Listing files from {INPUT_PATH}...")
    input_files = sorted(list_gcs_parquet_files(INPUT_PATH))
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
    
    workers = []
    for gpu_id in range(NUM_GPUS):
        p = mp.Process(
            target=gpu_worker,
            args=(
                gpu_id,
                file_queue,
                result_queue,
                BATCH_SIZE,
                OUTPUT_PATH,
                MODEL,
                CONFIDENCE_THRESHOLD
            )
        )
        p.start()
        workers.append(p)
        print(f"Started worker for GPU {gpu_id}")
    
    for file_idx, parquet_file in files_to_process:
        file_queue.put((file_idx, parquet_file))
    
    for _ in workers:
        file_queue.put(None)
    
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
        
        if results_received % 5 == 0:
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
    main()