"""
KG Builder for CC-News (GCS -> Relik -> LLM -> Neo4j Aura)

Features:
- Reads cleaned CC-News parquet files from GCS
- Extracts entities & relations via Relik
- Extracts events via a small LLM (Flan-T5-Small)
- Inserts nodes/edges/events into Neo4j Aura
- Filters relations by confidence threshold
- Supports batching for Neo4j inserts
"""

import os
import re
import json
import logging
import fsspec
import pyarrow.parquet as pq
from relik import Relik
from transformers import pipeline
from neo4j import GraphDatabase
import gc
import torch
from collections import defaultdict
from dotenv import load_dotenv
from itertools import combinations
import os

load_dotenv()
# ----------------- CONFIG ----------------- #
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
GCS_BUCKET = os.getenv("GCS_BUCKET")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-creds.json"

RELATION_CONF_THRESHOLD = 0.25  # min confidence
BATCH_SIZE_NEO4J = 1000        # bulk insert entities/relations
BATCH_SIZE_RELIK = 16           # batch inference for Relik
ROW_GROUP_BATCH_SIZE = 40       # for parquet chunking
DAY_PATTERN = re.compile(r"year=(\d+)/month=(\d+)/day=(\d+)")

# ----------------- RUN WINDOW ----------------- #
START_DAY = "2025-11-03"
END_DAY   = "2025-11-05"   # run 5 days at a time

# ----------------- LOGGING ----------------- #
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ----------------- SETUP FILESYSTEM ----------------- #
fs = fsspec.filesystem("gs")

def find_parquet_files(bucket_prefix):
    """Recursively find all *_cleaned.parquet files under bucket_prefix"""
    all_files = fs.find(bucket_prefix, withdirs=False)
    return [f for f in all_files if f.endswith("_cleaned.parquet")]

files = find_parquet_files("gs://cleaned-gen-ai-tu/news/raw/")
logger.info(f"Found {len(files)} cleaned parquet files.")

for f in files[:5]:  # Log first 5 files found
    logger.info(f"  - {f}")

def read_parquet_gcs(path):
    """Read a parquet file from GCS into pandas DataFrame."""
    with fs.open(path, "rb") as f:
        return pq.read_table(f).to_pandas()

def read_parquet_gcs_chunked(path, row_group_batch_size=3):
    """Yield row group chunks from parquet to save memory."""
    with fs.open(path, "rb") as f:
        parquet_file = pq.ParquetFile(f)
        total_row_groups = parquet_file.num_row_groups

        for i in range(0, total_row_groups, row_group_batch_size):
            row_groups = list(range(i, min(i + row_group_batch_size, total_row_groups)))
            table = parquet_file.read_row_groups(row_groups)
            yield table.to_pandas()

# ----------------- SETUP MODELS ----------------- #
logger.info("Loading Relik IE model...")
relik = Relik.from_pretrained(
    "relik-ie/relik-cie-large",
    task="SPAN",          # ✅ entity-only
    device="cuda",
    precision="fp16"
)

# ----------------- SETUP NEO4J ----------------- #
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# ----------------- EXTRACTION FUNCTIONS ----------------- #
def extract_entities_relations(text, min_conf=RELATION_CONF_THRESHOLD):
    """Extract entities and relations with confidence filtering."""
    out = relik(text)
    outputs = out if isinstance(out, list) else [out]
    entities = {}
    relations = []

    for out in outputs:
        for span in out.spans:
            entities[span.text] = {"type": span.label}

        for t in out.triplets:
            if t.confidence >= min_conf:
                relations.append({
                    "subject": t.subject.text,
                    "relation": t.label,
                    "object": t.object.text,
                    "confidence": t.confidence
                })

    return entities, relations

def extract_entities_relations_batch(texts, min_conf=RELATION_CONF_THRESHOLD):
    """Batch inference with Relik on a list of texts."""
    out_batch = relik(texts)
    all_entities_list = []
    all_relations_list = []
    outputs = out_batch if isinstance(out_batch, list) else [out_batch]

    for out in outputs:
        entities = {span.text.lower(): {"type": span.label} for span in out.spans}

        relations = [
            {"src": t.subject.text.lower(), "dst": t.object.text.lower(), "relation": t.label, "confidence": t.confidence}
            for t in out.triplets if t.confidence >= min_conf
        ]

        # Co-occurrence
        relations += [
            {"src": a, "dst": b, "relation": "CO_OCCURS_WITH", "confidence": 1.0}
            for a, b in combinations(entities.keys(), 2)
        ]

        all_entities_list.append(entities)
        all_relations_list.append(relations)

    return all_entities_list, all_relations_list

def build_cooccurrence_relations(entities):
    return [
        {"subject": a, "relation": "CO_OCCURS_WITH", "object": b, "confidence": 1.0}
        for a, b in combinations(entities.keys(), 2)
    ]


# ----------------- NEO4J INSERTION ----------------- #
def insert_kg_batch(entity_list, relation_list):
    """Bulk insert entities and relations to Neo4j."""
    if not entity_list and not relation_list:
        return

    with driver.session() as session:
        if entity_list:
            session.run("""
                UNWIND $batch AS item
                MERGE (e:Entity {name: item.name})
                SET e.type = item.type
            """, batch=entity_list)

        if relation_list:
            session.run("""
                UNWIND $batch AS item
                MATCH (a:Entity {name: item.src})
                MATCH (b:Entity {name: item.dst})
                MERGE (a)-[:RELATED_TO]->(b)
            """, batch=relation_list)


# ----------------- HELPERS ----------------- #
def extract_day(path: str):
    m = DAY_PATTERN.search(path)
    if not m:
        return None
    y, mth, d = m.groups()
    return f"{y}-{mth}-{d}"

# ----------------- GROUP FILES BY DAY ----------------- #
files_by_day = defaultdict(list)

for f in files:
    day = extract_day(f)
    if day:
        files_by_day[day].append(f)

all_days = sorted(files_by_day.keys())
logger.info(f"Found {len(all_days)} days")

# ----------------- SELECT RUN WINDOW ----------------- #
selected_days = [
    d for d in all_days
    if START_DAY <= d <= END_DAY
]

logger.info(f"Processing days: {selected_days}")

# ----------------- MAIN PROCESSING LOOP ----------------- #
PROGRESS_FILE = "processed_days.json"

if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE) as f:
        processed_days = set(json.load(f))
else:
    processed_days = set()

for day in selected_days:
    if day in processed_days:
        logger.info(f"Skipping already processed day {day}")
        continue

    logger.info(f"==== Processing day {day} ====")
    for file in files_by_day[day]:
        logger.info(f"Processing {file}")

        all_entities, all_relations = [], []
        article_count = 0

        for df_chunk in read_parquet_gcs_chunked(file, row_group_batch_size=ROW_GROUP_BATCH_SIZE):
            df_chunk = df_chunk.drop_duplicates(subset="text")
            texts = [t.strip() for t in df_chunk["text"] if isinstance(t, str) and t.strip()]

            # Batch texts for Relik
            for i in range(0, len(texts), BATCH_SIZE_RELIK):
                batch_texts = texts[i:i+BATCH_SIZE_RELIK]

                #  Batch inference
                entities_list, relations_list = extract_entities_relations_batch(batch_texts)

                for entities, relations in zip(entities_list, relations_list):
                    for name, meta in entities.items():
                        all_entities.append({"name": name, "type": meta["type"]})
                    for r in relations:
                        all_relations.append({"src": r["src"], "dst": r["dst"]})

                    article_count += 1
                    if article_count % 100 == 0:
                        logger.info(f"{day}: {article_count} articles processed")

                # Bulk insert to Neo4j every BATCH_SIZE_NEO4J articles
                if len(all_entities) >= BATCH_SIZE_NEO4J:
                    insert_kg_batch(all_entities, all_relations)
                    all_entities, all_relations = [], []

            #  Clean GPU once per parquet chunk
            torch.cuda.empty_cache()
            gc.collect()

        # Insert remaining entities/relations after file
        insert_kg_batch(all_entities, all_relations)

    # Mark day complete
    processed_days.add(day)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(sorted(processed_days), f)

    logger.info(f" Finished day {day}")