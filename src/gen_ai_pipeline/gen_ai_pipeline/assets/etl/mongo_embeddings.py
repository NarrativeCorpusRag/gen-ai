from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_pyspark import PySparkResource
from pyspark.sql import functions as F
from gen_ai_pipeline.resources.mongo import MongoDBUploadConfig, write_to_mongodb_spark_connector
from dagster import MonthlyPartitionsDefinition
from gen_ai_pipeline.assets.etl.ccnews_chunk_embedding import ccnews_embeddings

monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01")

@asset(
    compute_kind="pyspark",
    group_name="mongodb_upload",
    partitions_def=monthly_partitions,
    description="Upload chunks using Spark MongoDB Connector (faster for large data)",
    deps=[ccnews_embeddings]
)
def mongodb_chunks_connector(
    context: AssetExecutionContext,
    config: MongoDBUploadConfig,
    pyspark: PySparkResource,
) -> MaterializeResult:
    """
    Upload chunks using the official Spark MongoDB Connector.
    
    Faster than mapInPandas for large datasets.
    
    Requires Spark to be configured with:
    spark.jars.packages=org.mongodb.spark:mongo-spark-connector_2.12:10.2.0
    """
    pk = context.partition_key
    year = int(pk[0:4])
    month = int(pk[5:7])
    
    spark = pyspark.spark_session
    
    df = spark.read.parquet(config.embeddings_path).filter(
        (F.col("year") == year) & (F.col("month") == month)
    )
    
    # Add _id field
    df_upload = df.select(
        F.col("chunk_id").alias("_id"),
        F.col("chunk_id"),
        F.col("doc_id"),
        F.col("chunk_index"),
        F.col("chunk_text"),
        F.col("embedding"),
        F.col("uri"),
        F.col("host"),
        F.col("year"),
        F.col("month"),
    )
    
    total_count = df_upload.count()
    
    write_to_mongodb_spark_connector(
        df_upload,
        config.mongodb_uri,
        config.database_name,
        "chunks",
        mode="append"  # Use append to avoid overwriting other partitions
    )
    
    return MaterializeResult(
        metadata={
            "total_chunks": MetadataValue.int(total_count),
            "year": MetadataValue.int(year),
            "month": MetadataValue.int(month),
        }
    )