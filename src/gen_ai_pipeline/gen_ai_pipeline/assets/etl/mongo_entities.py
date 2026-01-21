from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_pyspark import PySparkResource
from pyspark.sql import functions as F
from gen_ai_pipeline.resources.mongo import MongoDBUploadConfig, write_to_mongodb_spark_connector
from dagster import MonthlyPartitionsDefinition
from gen_ai_pipeline.assets.etl.ccnews_graph import graph_extraction

monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01")


@asset(
    compute_kind="pyspark",
    group_name="mongodb_upload",
    partitions_def=monthly_partitions,
    description="Upload entities to MongoDB using PySpark",
    deps=[graph_extraction]
)
def mongodb_entities_spark(
    context: AssetExecutionContext,
    config: MongoDBUploadConfig,
    pyspark: PySparkResource,
) -> MaterializeResult:
    """
    Upload entities from Relik extraction to MongoDB.
    
    Schema in MongoDB:
    - _id: entity_id
    - chunk_id: reference to parent chunk
    - text: entity surface text
    - label: entity type (ORG, PERSON, etc.)
    - wikipedia_id: Wikipedia identifier for knowledge linking
    """
    pk = context.partition_key
    year = int(pk[0:4])
    month = int(pk[5:7])
    
    spark = pyspark.spark_session
    
    context.log.info(f"Reading entities for {year}-{month}")
    
    # Read entities - they might not be partitioned by year/month
    # so we need to join with chunks to filter
    df = spark.read.parquet(config.entities_path)
    
    # If you have year/month in entities, filter directly
    # Otherwise, join with chunks to get the partition
    
    # Select needed columns
    df_upload = df.select(
        F.col("entity_id"),
        F.col("chunk_id"),
        F.col("text"),
        F.col("label"),
        F.col("wikipedia_id"),
        F.col("start"),
        F.col("end"),
    )
    
    total_count = df_upload.count()
    context.log.info(f"Uploading {total_count} entities to MongoDB")
       
    write_to_mongodb_spark_connector(
        df_upload,
        config.mongodb_uri,
        config.database_name,
        "entities",
        mode="append"  # Use append to avoid overwriting other partitions
    )
    
    return MaterializeResult(
        metadata={
            "total_entities": MetadataValue.int(total_count),
            "collection": MetadataValue.text("entities"),
        }
    )
