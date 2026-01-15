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
import json
import logging
import fsspec
import pyarrow.parquet as pq
from relik import Relik
from transformers import pipeline
from neo4j import GraphDatabase

# ----------------- CONFIG ----------------- #
GCS_BUCKET = "gs://cleaned-gen-ai-tu/"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gen-ai/src/research/jacob/gcp-creds.json"


NEO4J_URI = "neo4j+s://3ecc3ec1.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASS = "n38svvm7nXoFUn5V8zOq-PhZFnO7FfM_PTIJ5PWl82Y"

RELATION_CONF_THRESHOLD = 0.75
BATCH_SIZE = 100  # number of entities/edges per transaction

# ----------------- LOGGING ----------------- #
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ----------------- SETUP FILESYSTEM ----------------- #
fs = fsspec.filesystem("gs")
files = [f for f in fs.find(GCS_BUCKET) if f.endswith("_cleaned.parquet")]
logger.info(f"Found {len(files)} cleaned parquet files.")
for f in files[:5]:  # Log first 5 files found
    logger.info(f"  - {f}")

def read_parquet_gcs(path):
    """Read a parquet file from GCS into pandas DataFrame."""
    with fs.open(path, "rb") as f:
        return pq.read_table(f).to_pandas()

# ----------------- SETUP MODELS ----------------- #
logger.info("Loading Relik IE model...")
relik = Relik.from_pretrained(
    "relik-ie/relik-cie-small",
    device="cuda",      # Lightning GPU
    index_device="cpu",
    precision="fp16",
    topk=50
)

logger.info("Loading event extraction LLM...")
event_extractor = pipeline(
    "text2text-generation",
    model="google/flan-t5-small",
    device=0
)

# ----------------- SETUP NEO4J ----------------- #
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# ----------------- EXTRACTION FUNCTIONS ----------------- #
def extract_entities_relations(text, min_conf=RELATION_CONF_THRESHOLD):
    """Extract entities and relations with confidence filtering."""
    out = relik(text)
    entities = {span.text: {"type": span.label, "start": span.start, "end": span.end} for span in out.spans}
    relations = [
        {"subject": t.subject.text, "relation": t.label, "object": t.object.text, "confidence": t.confidence}
        for t in out.triplets if t.confidence >= min_conf
    ]
    return entities, relations

def extract_events(text):
    """Extract events from text using LLM."""
    prompt = f"""
    Extract all events from the following text. 
    For each event, return JSON with:
    - event_type
    - trigger_sentence
    - participants (entities involved)
    - location (if any)
    - time (if any)
    Text: {text}
    """
    llm_output = event_extractor(prompt, max_length=1024)
    try:
        events = json.loads(llm_output[0]['generated_text'])
    except json.JSONDecodeError:
        events = llm_output[0]['generated_text']
    return events

# ----------------- NEO4J INSERTION ----------------- #
def insert_kg_to_neo4j(entities, relations, events=None):
    """Insert entities, relations, and optional events into Neo4j with batching."""
    with driver.session() as session:
        # Batch entities
        entity_items = list(entities.items())
        for i in range(0, len(entity_items), BATCH_SIZE):
            batch = entity_items[i:i+BATCH_SIZE]
            tx = session.begin_transaction()
            for name, meta in batch:
                tx.run(
                    "MERGE (e:Entity {name: $name}) "
                    "SET e.type = $etype",
                    name=name, etype=meta["type"]
                )
            tx.commit()

        # Batch relations
        for i in range(0, len(relations), BATCH_SIZE):
            batch = relations[i:i+BATCH_SIZE]
            tx = session.begin_transaction()
            for r in batch:
                tx.run(
                    """
                    MATCH (a:Entity {name: $sub})
                    MATCH (b:Entity {name: $obj})
                    MERGE (a)-[rel:RELATION {label: $label}]->(b)
                    SET rel.confidence = $conf
                    """,
                    sub=r["subject"], obj=r["object"], label=r["relation"], conf=r["confidence"]
                )
            tx.commit()

        # Insert events and link participants
        if events:
            for event in events:
                trigger = event.get("trigger_sentence", "")
                tx = session.begin_transaction()
                tx.run(
                    "MERGE (ev:Event {trigger:$trigger}) "
                    "SET ev.type=$etype, ev.location=$loc, ev.time=$time",
                    trigger=trigger,
                    etype=event.get("event_type", ""),
                    loc=event.get("location", ""),
                    time=event.get("time", "")
                )
                # Link participants
                for p in event.get("participants", []):
                    tx.run(
                        """
                        MATCH (ev:Event {trigger: $trigger})
                        MERGE (ent:Entity {name: $p})
                        MERGE (ent)-[:PARTICIPATES_IN]->(ev)
                        """,
                        trigger=trigger,
                        p=p
                    )
                tx.commit()

# ----------------- MAIN LOOP ----------------- #
for file in files:
    logger.info(f"Processing {file} ...")
    df = read_parquet_gcs(file)
    
    for text in df["text"]:
        if not text.strip():
            continue
        entities, relations = extract_entities_relations(text)
        events = extract_events(text)
        insert_kg_to_neo4j(entities, relations, events)

logger.info("All files processed. Knowledge Graph built in Neo4j Aura!")
