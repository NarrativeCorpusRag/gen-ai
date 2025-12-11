from dagster import Definitions, load_assets_from_modules
import os 
from gen_ai_pipeline.index_asset import cc_news_index  # noqa: TID252
from dagster_polars import PolarsParquetIOManager
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

def get_gcs_options():
    return {
        "project": os.getenv('GCP_PROJECT'),
        "token": {
            "type": "service_account",
            "project_id": os.getenv('GCP_PROJECT'),
            "private_key_id": os.getenv('GCP_PRIVATE_KEY_ID'),
            "private_key": os.getenv('GCP_PRIVATE_KEY').replace('\\n', '\n') if os.getenv('GCP_PRIVATE_KEY') else None,
            "client_email": os.getenv('GCP_EMAIL'),
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }
    
defs = Definitions(
    assets=[cc_news_index],
    resources={
        "gcs_parquet_io_manager": PolarsParquetIOManager(
            base_dir="gs://gen-ai-tu/",
            storage_options=get_gcs_options()
        )
    }
)