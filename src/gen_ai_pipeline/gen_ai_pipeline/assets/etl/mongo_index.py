from pymongo import MongoClient
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)
from gen_ai_pipeline.resources.mongo import MongoDBUploadConfig 
from gen_ai_pipeline.assets.etl.mongo_embeddings import mongodb_chunks_connector
from gen_ai_pipeline.assets.etl.mongo_entities import mongodb_entities_spark
from gen_ai_pipeline.assets.etl.mongo_relations import mongodb_relations_spark

@asset(
    compute_kind="mongodb",
    group_name="mongodb_upload",
    description="Create MongoDB indexes for efficient querying",
    deps=[mongodb_chunks_connector, mongodb_entities_spark, mongodb_relations_spark]
)
def mongodb_indexes(
    context: AssetExecutionContext,
    config: MongoDBUploadConfig,
) -> MaterializeResult:
    """
    Create indexes on MongoDB collections.
    
    Note: Vector Search index must be created manually in Atlas UI.
    """
    
    client = MongoClient(config.mongodb_uri)
    db = client[config.database_name]
    
    # Chunks indexes
    context.log.info("Creating chunks indexes...")
    chunks = db["chunks"]
    chunks.create_index("chunk_id", unique=True)
    chunks.create_index("doc_id")
    chunks.create_index([("year", 1), ("month", 1)])
    chunks.create_index("host")
    
    # Entities indexes
    context.log.info("Creating entities indexes...")
    entities = db["entities"]
    entities.create_index("entity_id", unique=False)
    entities.create_index("chunk_id")
    entities.create_index("text")
    entities.create_index("wikipedia_id")
    
    # Relations indexes
    context.log.info("Creating relations indexes...")
    relations = db["relations"]
    relations.create_index("relation_id", unique=False)
    relations.create_index("chunk_id")
    relations.create_index("head_text")
    relations.create_index("tail_text")
    relations.create_index("relation")
    relations.create_index([("head_text", 1), ("relation", 1)])
    relations.create_index([("tail_text", 1), ("relation", 1)])
    
    client.close()
    
    context.log.info("""
    This Doesn't Create Vector Search index
    
    1. Go to Atlas UI → Database → Search
    2. Create Search Index → JSON Editor
    3. Index Name: vector_index
    4. Collection: chunks
    5. Paste this definition:
    
    {
      "fields": [
        {
          "type": "vector",
          "path": "embedding",
          "numDimensions": 1024,
          "similarity": "cosine"
        },
        {"type": "filter", "path": "year"},
        {"type": "filter", "path": "month"},
        {"type": "filter", "path": "host"}
      ]
    }
    """)
    
    return MaterializeResult(
        metadata={
            "chunks_indexes": MetadataValue.int(4),
            "entities_indexes": MetadataValue.int(4),
            "relations_indexes": MetadataValue.int(6),
            "note": MetadataValue.text("Vector Search index must be created in Atlas UI"),
        }
    )