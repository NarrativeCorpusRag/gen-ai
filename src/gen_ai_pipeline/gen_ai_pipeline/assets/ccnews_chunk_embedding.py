import os
import torch
import pandas as pd
from typing import Iterator
from dagster import (
    asset, MonthlyPartitionsDefinition, Config
)
from dagster_pyspark import PySparkResource
from pyspark.sql.types import ArrayType, FloatType
from gen_ai_pipeline.assets.ccnews_chunking import ccnews_chunking

monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01")
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
def compute_embeddings_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """
    Initializes model once per partition (thread) and runs inference.
    """
    from FlagEmbedding import FlagAutoModel
    
    # Initialize Model (Uses GPU)
    model = FlagAutoModel.from_finetuned(
        os.environ["EMBEDDING_MODEL_NAME"],
        query_instruction_for_retrieval="Represent this sentence for searching relevant passages:",
        use_fp16=True
    )
    
    for doc in iterator:
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        # Batch size 256 fits well on 40GB GPU with 4 parallel streams we have in local, might want to change that if you have different GPU
        embeddings = model.encode(doc['chunk_text'].tolist(), batch_size=256)
        
        doc['embedding'] = embeddings.tolist()
        yield doc
class EmbeddingConfig(Config):
    input_path: str = "gs://gen-ai-tu/news/chunks/"
    output_path: str = "gs://gen-ai-tu/news/embeddings/"
    partitions: int = 1000

@asset(
    compute_kind="pyspark",
    group_name="pipeline",
    deps=[ccnews_chunking],
    partitions_def=monthly_partitions,
)
def local_news_embeddings(context, config: EmbeddingConfig, pyspark: PySparkResource):
    """
    Local embedding generation using PySparkResource.
    """
    pk = context.partition_key
    # Extract year and month from partition key
    year = int(pk[0:4])
    month = int(pk[5:7])
    spark = pyspark.spark_session
    
    context.log.info(f"Reading from: {config.input_path}")
    
    df = spark.read.parquet(config.input_path).filter(
        (spark.col("year") == year) & (spark.col("month") == month)
    )
    
    output_schema = df.schema.add("embedding", ArrayType(FloatType()))

    df_transformed = (
        df.repartition(config.partitions)
        .mapInPandas(compute_embeddings_partition, schema=output_schema)
    )
    
    context.log.info(f"Writing to: {config.output_path}")
    
    (df_transformed.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(config.output_path)
    )
    
    return config.output_path