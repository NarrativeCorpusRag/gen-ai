import re
from urllib.parse import urlparse
import gzip
import boto3
import idna
import tldextract
from fastwarc.warc import ArchiveIterator, WarcRecordType, is_http
from fastwarc.stream_io import GZipStream
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.lang import detect_fast
from resiliparse.parse.encoding import detect_encoding, bytes_to_str
from surt import surt

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, FloatType
)
from dagster_pipes import (
    PipesContext,
    PipesCliArgsParamsLoader,
    PipesGCSContextLoader,
    PipesGCSMessageWriter,
    open_dagster_pipes,
)
from google.cloud.storage import Client as GCSClient


ip_pattern = re.compile(r"^(?:www\.)?\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\Z")
host_part_pattern = re.compile(r"^[a-z0-9]([a-z0-9_-]{0,61}[a-z0-9])?\Z", re.IGNORECASE | re.ASCII)


def get_surt_host(url):  # noqa: C901
    extracted = tldextract.extract(url, include_psl_private_domains=True)
    registered_domain = extracted.top_domain_under_public_suffix

    if registered_domain == "":
        registered_domain = f"{extracted.subdomain}.{extracted.domain}"
        if registered_domain == "":
            host = urlparse(url).hostname
            if not host:
                return None
        else:
            host = registered_domain
    else:
        host = registered_domain

    host = host.strip().lower()
    if len(host) < 1 or len(host) > 253:
        return None
    if ip_pattern.match(host):
        return None
    parts = host.split(".")
    if parts[-1] == "":
        parts = parts[0:-1]
    if len(parts) <= 1:
        return None
    if len(parts) > 2 and parts[0] == "www":
        parts = parts[1:]
    for i, part in enumerate(parts):
        if len(part) > 63:
            return None
        if not host_part_pattern.match(part):
            try:
                idn = idna.encode(part).decode("ascii")
            except Exception:
                return None
            if host_part_pattern.match(idn):
                parts[i] = idn
            else:
                return None
    parts.reverse()
    return ".".join(parts)


def process_warc_partition(iterator, aws_access_key, aws_secret_key):
    """
        This runs on the worker nodes. 
        It receives an iterator of rows (each row contains a 'warc_path').
        It initializes its own boto3 client and processes files.
    """
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )
    
    for row in iterator:
        key = row.warc_path

        try:
            path_parts = key.split('/')
            filename = path_parts[-1]
            year_str = path_parts[-3] 
            month_str = path_parts[-2]
            timestamp = filename.split('-')[2]
            day_str = timestamp[6:8]
        except Exception:
            # print(f"Skipping malformed path: {key}")
            continue

        try:
            response = s3_client.get_object(Bucket='commoncrawl', Key=key)
            stream = GZipStream(response['Body'])
            
            for record in ArchiveIterator(stream, record_types=WarcRecordType.response, func_filter=is_http):
                try:
                    uri = record.headers.get('WARC-Target-URI')
                    body_bytes = record.reader.read()
                    
                    encoding = detect_encoding(body_bytes)
                    html = bytes_to_str(body_bytes, encoding)
                    text = extract_plain_text(html)
                    
                    # fastwarc returns datetime objects that may contain timezone info
                    # which Spark's default pickler often struggles with.
                    http_date_obj = record.http_date
                    http_date = str(http_date_obj) if http_date_obj else None

                    http_last_mod_obj = record.http_last_modified
                    http_last_modified = str(http_last_mod_obj) if http_last_mod_obj else None

                    http_charset = record.http_charset
                    surt_uri = surt(uri)
                    host = get_surt_host(uri)
                    
                    r = detect_fast(text, n_results=3)
                    main_lang = r[0][0] if r else 'unknown'
                    langs = [x[0] for x in r]
                    confs = [float(x[1]) for x in r]

                    yield {
                        'uri': uri,
                        'text': text,
                        'html': html,
                        'main_lang': main_lang,
                        'langs': langs,
                        'confs': confs,
                        'http_date': http_date,
                        'http_last_modified': http_last_modified,
                        'http_charset': http_charset,
                        'surt_uri': surt_uri,
                        'host': host,
                        'path': filename,
                        'year': year_str,
                        'month': month_str,
                        'day': day_str
                    }
                except Exception:
                    continue
                    
        except Exception as e:
            print(f"Error processing WARC file {key}: {e}")

def main():
    gcs_client = GCSClient()

    with open_dagster_pipes(
        context_loader=PipesGCSContextLoader(client=gcs_client),
        message_writer=PipesGCSMessageWriter(client=gcs_client),
        params_loader=PipesCliArgsParamsLoader(),
    ) as pipes:
        # parameters still come from argparse (your current approach), OR from pipes.get_params()
        pipes.log.info("Starting CC-NEWS extract job")
        context = PipesContext.get()
        year = context.get_extra("year")
        month = context.get_extra("month")
        out_root = context.get_extra("out_root")
        aws_access_key = context.get_extra("ASCII_AWS_ACCESS_KEY_ID")
        aws_secret_key = context.get_extra("ASCII_AWS_SECRET_ACCESS_KEY")
        if not aws_access_key or not aws_secret_key:
            raise RuntimeError("Missing ASCII_AWS_ACCESS_KEY_ID / ASCII_AWS_SECRET_ACCESS_KEY env vars")
        pipes.log.info(f"Starting CC-NEWS extract for {year}-{month}")

        spark = SparkSession.builder.appName("CC-NEWS").getOrCreate()

        s3_client = boto3.client('s3', 
                                 aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key)
            
        paths_gz_key = f'crawl-data/CC-NEWS/{year}/{month}/warc.paths.gz'
        response = s3_client.get_object(Bucket='commoncrawl', Key=paths_gz_key)
        decompressed_bytes = gzip.decompress(response['Body'].read())
            
            # Read paths into a list
        warc_paths = [line.decode('utf-8').strip() for line in decompressed_bytes.splitlines()]
            
            # B. Create a simple DataFrame of paths to distribute work
            # Repartition determines parallelism. e.g., if you have 1000 files and 100 partitions, 
            # each task processes ~10 files.
        paths_df = spark.createDataFrame([(p,) for p in warc_paths], ["warc_path"]).repartition(100)

            # C. Define Output Schema
        schema = StructType([
            StructField("uri", StringType(), True),
            StructField("text", StringType(), True),
            StructField("html", StringType(), True),
            StructField("main_lang", StringType(), True),
            StructField("langs", ArrayType(StringType()), True),
            StructField("confs", ArrayType(FloatType()), True),
            StructField("http_date", StringType(), True),
            StructField("http_last_modified", StringType(), True),
            StructField("http_charset", StringType(), True),
            StructField("surt_uri", StringType(), True),
            StructField("host", StringType(), True),
            # Partition cols
            StructField("path", StringType(), True),
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
        ])

            # D. Execute Processing (Map Partitions)
            # mapPartitions is more efficient than map because we init the S3 client once per partition
        processed_rdd = paths_df.rdd.mapPartitions(
            lambda iterator: process_warc_partition(iterator, aws_access_key, aws_secret_key)
        )
            
            # Convert back to DataFrame
        final_df = spark.createDataFrame(processed_rdd, schema=schema)
        final_df.show()
        # E. Write to GCS
        pipes.log.info(f"Writing to {out_root}...")
        final_df.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day", "path", "main_lang") \
            .parquet(out_root)
        pipes.log.info("Job Complete.")
        pipes.report_asset_materialization(
            metadata={
                "year": year,
                "month": month,
                "out_root": out_root,
            }
        )
        pipes.log.info("Done.")
        spark.stop()


if __name__ == "__main__":
    main()