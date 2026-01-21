from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)
from gen_ai_pipeline.resources.mongo import MongoDBUploadConfig 
from dagster import MonthlyPartitionsDefinition
from dagster_pyspark import PySparkResource
from gen_ai_pipeline.assets.etl.ccnews_graph import graph_extraction


monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01")

@asset(
    compute_kind="pyspark",
    group_name="mongodb_upload",
    partitions_def=monthly_partitions,
    description="Upload relations to MongoDB using PySpark",
    deps=[graph_extraction]
)
def mongodb_relations_spark(
    context: AssetExecutionContext,
    config: MongoDBUploadConfig,
    pyspark: PySparkResource,
) -> MaterializeResult:
    """
    Upload relations (triplets) from Relik extraction to MongoDB.
    
    Schema in MongoDB:
    - _id: relation_id
    - chunk_id: source chunk
    - head_text: subject entity text
    - relation: relationship type
    - tail_text: object entity text
    - confidence: extraction confidence score
    """
    pk = context.partition_key
    year = int(pk[0:4])
    month = int(pk[5:7])
    
    spark = pyspark.spark_session
    
    context.log.info(f"Reading relations for {year}-{month}")
    
    df = spark.read.parquet(config.relations_path)
    
    df_upload = df.select(
        F.col("relation_id"),
        F.col("chunk_id"),
        F.col("head_text"),
        F.col("head_id"),
        F.col("head_wikipedia_id"),
        F.col("relation"),
        F.col("tail_text"),
        F.col("tail_id"),
        F.col("tail_wikipedia_id"),
        F.col("confidence"),
    )
    
    total_count = df_upload.count()
    context.log.info(f"Uploading {total_count} relations to MongoDB")
    
    result_schema = StructType([
        StructField("records_written", IntegerType(), True)
    ])
    
    result_df = df_upload.mapInPandas(
        lambda it: write_to_mongodb_partition(
            it,
            config.mongodb_uri,
            config.database_name,
            "relations",
            "relation_id"
        ),
        schema=result_schema
    )
    
    total_written = result_df.agg(F.sum("records_written")).collect()[0][0]
    
    context.log.info(f"Successfully uploaded {total_written} relations")
    
    return MaterializeResult(
        metadata={
            "total_relations": MetadataValue.int(total_count),
            "collection": MetadataValue.text("relations"),
        }
    )

