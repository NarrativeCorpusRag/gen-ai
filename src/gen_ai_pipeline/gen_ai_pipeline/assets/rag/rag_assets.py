import os
from dagster import (
    asset, 
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
)
from FlagEmbedding import FlagAutoModel
from gen_ai_pipeline.helpers.rag_helpers import RAGQueryEngine
from gen_ai_pipeline.assets.etl.mongo_index import mongodb_indexes

class RAGQueryConfig(Config):
    """Configuration for RAG query asset"""
    question: str
    method: str = "hybrid"  # "traditional", "graph", or "hybrid"
    top_k: int = 5
    llm_provider: str = "gemini"  # or "anthropic"
    llm_model: str = "gemini-2.5-flash"
    generate_answer: bool = False


@asset(
    compute_kind="rag",
    group_name="rag_query",
    description="Execute RAG query against MongoDB",
    deps=[mongodb_indexes]
)
def rag_query(
    context: AssetExecutionContext,
    config: RAGQueryConfig,
) -> MaterializeResult:
    """
    Execute a RAG query using the specified method.
    
    This asset demonstrates how to query the system.
    In production, you'd typically call this from an API endpoint.
    """
    mongodb_uri = os.environ["MONGODB_URI"]
    database_name = "DataScience"
    # Initialize embedding model
    embedding_model = FlagAutoModel.from_finetuned(
        os.environ.get("EMBEDDING_MODEL_NAME", "BAAI/bge-base-en-v1.5"),
        query_instruction_for_retrieval="Represent this sentence for searching relevant passages:",
        use_fp16=True
    )
    
    # Initialize query engine
    engine = RAGQueryEngine(
        mongodb_uri=mongodb_uri,
        database_name=database_name,
        embedding_model=embedding_model,
        llm_provider=config.llm_provider,
        llm_model=config.llm_model,
    )
    
    # Execute query based on method
    if config.method == "traditional":
        result = engine.traditional_rag(config.question, top_k=config.top_k, generate_answer=config.generate_answer)
    elif config.method == "graph":
         result = engine.graph_rag(config.question, max_chunks=config.top_k, generate_answer=config.generate_answer) 
    else:  # hybrid
        result = engine.hybrid_rag(config.question, top_k_chunks=config.top_k, generate_answer=config.generate_answer)
    
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