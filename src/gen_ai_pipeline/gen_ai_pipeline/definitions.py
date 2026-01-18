import os
import dotenv
from dagster import Definitions

from dagster_polars import PolarsParquetIOManager

from gen_ai_pipeline.assets.index_asset import cc_news_index
from gen_ai_pipeline.assets.ccnews_collect import ccnews_html_text_month
from gen_ai_pipeline.assets.ccnews_preprocess import ccnews_preprocess
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


defs = Definitions(
    assets=[cc_news_index, ccnews_html_text_month, ccnews_preprocess],
    resources={
        "gcs_parquet_io_manager": PolarsParquetIOManager(
            base_dir="gs://gen-ai-tu/",
            storage_options=get_gcs_options(),
        ),
        # THIS is what fixes your error:
        "dataproc_job_client": make_dataproc_job_client(),
    },
)