import os
import gzip
import boto3
import pandas as pd
import polars as pl
from io import BytesIO
from dagster import (
    asset, 
    AssetExecutionContext,
    DynamicPartitionsDefinition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue
)
from gen_ai_pipeline.helpers.gcloud_helpers import create_folder

warc_partitions_def = DynamicPartitionsDefinition(name="warc_files")

import os
from google.cloud import storage

def upload_file_to_gcs(local_path: str, gcs_uri: str) -> None:
    assert gcs_uri.startswith("gs://")
    _, _, rest = gcs_uri.partition("gs://")
    bucket, _, key = rest.partition("/")
    client = storage.Client()
    client.bucket(bucket).blob(key).upload_from_filename(local_path)


    
@asset(
    key_prefix=["news"],
    name="index",
    description="Scrapes Common Crawl index and saves partitioned parquet to GCS",
    group_name="seeds",
    compute_kind="polars",
)
def cc_news_index(context: AssetExecutionContext) -> MaterializeResult:
    s3_client = boto3.client(
        's3', 
        region_name='us-east-1',
        aws_access_key_id=os.getenv('ASCII_AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('ASCII_AWS_SECRET_ACCESS_KEY')
    )
    key = 'crawl-data/CC-NEWS/index.html'
    context.log.info(f"Fetching years from {key}")
    
    res0 = s3_client.get_object(Bucket='commoncrawl', Key=key)
    years = pd.read_html(res0['Body'].read())

    index_list = []

    for i1, r1 in years[0].iterrows():
        year_int = int(r1['Year'])
        monthly_key = f'crawl-data/CC-NEWS/{year_int}/index.html'
        
        try:
            res1 = s3_client.get_object(Bucket='commoncrawl', Key=monthly_key)
            month = pd.read_html(res1['Body'].read())
            
            for i2, r2 in month[0].iterrows():
                warc_path_suffix = r2['WARC file list']
                monty_key = f'crawl-data/CC-NEWS/{year_int}/{warc_path_suffix}'
                
                try:
                    res2 = s3_client.get_object(Bucket='commoncrawl', Key=monty_key)
                    decompressed_bytes = gzip.decompress(res2['Body'].read())
                    data = pl.read_csv(BytesIO(decompressed_bytes), has_header=False)
                    
                    cleaned_data = data.with_columns(
                        pl.lit(year_int).alias('year'),
                        pl.lit(int(r2['Month'])).alias('month'),
                        pl.col('column_1').str.split('-').list.get(4).str.slice(6,2).cast(pl.Int8).alias('day')
                    ).rename({'column_1': 'warc_destination'})
                    
                    index_list.append(cleaned_data)
                    context.log.debug(f"Processed {monty_key}: {len(cleaned_data)} rows")

                except Exception as e:
                    context.log.warning(f"Failed to process {monty_key}: {e}")
        except Exception as e:
             context.log.warning(f"Failed to process year {year_int}: {e}")

    if not index_list:
        context.log.info("No data found.")
        return MaterializeResult(metadata={"row_count": 0})

    final_df = pl.concat(index_list)
    
    output_path = "gs://gen-ai-tu/index/"
    create_folder('index/')
    context.log.info(f"Writing {len(final_df)} rows to {output_path}...")
    
    final_df.write_parquet(
                output_path,
                partition_by=['year', 'month', 'day'],
                storage_options={
                    "project_id": os.getenv('GCP_PROJECT'),
                    "private_key_id": os.getenv('GCP_PRIVATE_KEY_ID'),
                    "private_key": os.getenv('GCP_PRIVATE_KEY'),
                    "client_email": os.getenv('GCP_EMAIL')},
                use_pyarrow=False
            )
    
    return MaterializeResult(
        metadata={
            "row_count": len(final_df),
            "output_path": MetadataValue.path(output_path),
            "preview": MetadataValue.md(str(final_df.head()))
        }
    )