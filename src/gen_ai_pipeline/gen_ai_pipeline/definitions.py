import os
import dotenv
from dagster import Definitions
from dagster import Definitions
from dagster_pyspark import PySparkResource

from dagster_polars import PolarsParquetIOManager

from gen_ai_pipeline.assets.etl.index_asset import cc_news_index
from gen_ai_pipeline.assets.etl.ccnews_collect import ccnews_html_text_month
from gen_ai_pipeline.assets.etl.ccnews_preprocess import ccnews_preprocess
from gen_ai_pipeline.assets.etl.ccnews_chunking import ccnews_chunking_asset
from gen_ai_pipeline.assets.etl.ccnews_chunk_embedding import ccnews_embeddings
from gen_ai_pipeline.assets.etl.ccnews_graph import graph_extraction
from gen_ai_pipeline.assets.etl.mongo_embeddings import mongodb_chunks_connector
from gen_ai_pipeline.assets.etl.mongo_entities import mongodb_entities_spark
from gen_ai_pipeline.assets.etl.mongo_index import mongodb_indexes
from gen_ai_pipeline.assets.etl.mongo_relations import mongodb_relations_spark
from gen_ai_pipeline.assets.rag.rag_assets import rag_query
from gen_ai_pipeline.resources.mongo import MongoDBResource

# Dagster Pipes + GCP
from dagster_gcp.pipes import (
    PipesDataprocJobClient,
    PipesGCSContextInjector,
    PipesGCSMessageReader,
)
from google.api_core.client_options import ClientOptions
from google.cloud.dataproc_v1 import JobControllerClient
from google.cloud.storage import Client as GCSClient

dotenv.load_dotenv(dotenv.find_dotenv())



def get_gcs_options():
    return {
        "project": os.getenv("GCP_PROJECT"),
        "token": {
            "type": "service_account",
            "project_id": os.getenv("GCP_PROJECT"),
            "private_key_id": os.getenv("GCP_PRIVATE_KEY_ID"),
            "private_key": os.getenv("GCP_PRIVATE_KEY").replace("\\n", "\n") if os.getenv("GCP_PRIVATE_KEY") else None,
            "client_email": os.getenv("GCP_EMAIL"),
            "token_uri": "https://oauth2.googleapis.com/token",
        },
    }


def make_dataproc_job_client() -> PipesDataprocJobClient:
    # Required env vars:
    #   DATAPROC_REGION (e.g. "europe-west3")
    #   PIPES_GCS_BUCKET (bucket name only, e.g. "gen-ai-tu-pipes")
    region = os.environ["GCP_CLUSTER_REGION"]
    pipes_bucket = os.environ["PIPES_GCS_BUCKET"]

    dataproc = JobControllerClient(
        client_options=ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")
    )
    gcs = GCSClient(project=os.environ.get("GCP_PROJECT"))

    return PipesDataprocJobClient(
        client=dataproc,
        context_injector=PipesGCSContextInjector(bucket=pipes_bucket, client=gcs),
        message_reader=PipesGCSMessageReader(
            bucket=pipes_bucket,
            client=gcs,
            include_stdio_in_messages=True,
        ),
    )

packages = [
    "io.delta:delta-spark_2.12:3.2.0",
    "org.postgresql:postgresql:42.7.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.apache.spark:spark-hadoop-cloud_2.12:3.5.0",
    "com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.6.0"
]
# Define the Resource
local_spark = PySparkResource(
    spark_config={
        # --- Concurrency ---
        # 4 threads to prevent GPU OOM. 
        "spark.master": "local[4]", 
        "spark.default.parallelism": "16",
        "spark.sql.shuffle.partitions": "16",
        
        # --- Memory (Driver takes all in local mode) ---
        "spark.driver.memory": "200g",
        "spark.driver.maxResultSize": "20g",
        "spark.driver.memoryOverhead": "50g",
        
        # --- Performance ---
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.rapids.sql.enabled": "true", # Disable RAPIDS to save VRAM for PyTorch

        
        # --- GCS / S3 Configs
        "spark.jars": "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.19/gcs-connector-hadoop3-2.2.19-shaded.jar",
        "spark.jars.packages": ",".join(packages),
        "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
    }
)


defs = Definitions(
    assets=[cc_news_index, ccnews_html_text_month, ccnews_preprocess, ccnews_chunking_asset, ccnews_embeddings, 
            graph_extraction, mongodb_chunks_connector, mongodb_entities_spark, mongodb_indexes, mongodb_relations_spark, rag_query],
    resources={
        "gcs_parquet_io_manager": PolarsParquetIOManager(
            base_dir="gs://gen-ai-tu/",
            storage_options=get_gcs_options(),
        ),
        "dataproc_job_client": make_dataproc_job_client(),
        "pyspark": local_spark,
        'mongodb': MongoDBResource,
    },
)