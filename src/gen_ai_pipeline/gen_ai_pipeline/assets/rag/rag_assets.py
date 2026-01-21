import os
from dagster import (
    asset, 
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
)
from gen_ai_pipeline.resources.mongo import MongoDBResource
from FlagEmbedding import FlagAutoModel
from gen_ai_pipeline.helpers.rag_helpers import RAGQueryEngine
from gen_ai_pipeline.assets.etl.mongo_index import mongodb_indexes

class RAGQueryConfig(Config):
    """Configuration for RAG query asset"""
    question: str
    method: str = "hybrid"  # "traditional", "graph", or "hybrid"
    top_k: int = 5


@asset(
    compute_kind="rag",
    group_name="rag_query",
    description="Execute RAG query against MongoDB",
    deps=[mongodb_indexes]
)
def rag_query(
    context: AssetExecutionContext,
    config: RAGQueryConfig,
    mongodb: MongoDBResource,
) -> MaterializeResult:
    """
    Execute a RAG query using the specified method.
    
    This asset demonstrates how to query the system.
    In production, you'd typically call this from an API endpoint.
    """
    
    # Initialize embedding model
    embedding_model = FlagAutoModel.from_finetuned(
        os.environ.get("EMBEDDING_MODEL_NAME", "BAAI/bge-base-en-v1.5"),
        query_instruction_for_retrieval="Represent this sentence for searching relevant passages:",
        use_fp16=True
    )
    
    # Initialize query engine
    engine = RAGQueryEngine(
        mongodb_uri=mongodb.connection_string,
        database_name=mongodb.database_name,
        embedding_model=embedding_model,
    )
    
    # Execute query based on method
    if config.method == "traditional":
        result = engine.traditional_rag(config.question, top_k=config.top_k)
    elif config.method == "graph":
        result = engine.graph_rag(config.question, top_k_chunks=config.top_k)
    else:  # hybrid
        result = engine.hybrid_rag(config.question, top_k_vector=config.top_k)
    
    engine.close()
    
    context.log.info(f"Query method: {config.method}")
    context.log.info(f"Retrieved {len(result.chunks)} chunks")
    context.log.info(f"Found {len(result.entities)} entities")
    context.log.info(f"Found {len(result.relations)} relations")
    
    return MaterializeResult(
        metadata={
            "question": MetadataValue.text(config.question),
            "method": MetadataValue.text(config.method),
            "chunks_retrieved": MetadataValue.int(len(result.chunks)),
            "entities_found": MetadataValue.int(len(result.entities)),
            "relations_found": MetadataValue.int(len(result.relations)),
            "context_preview": MetadataValue.md(result.context_text[:2000] + "..."),
        }
    )