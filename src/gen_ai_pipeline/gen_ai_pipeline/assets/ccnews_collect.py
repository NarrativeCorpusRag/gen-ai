from dagster import (
    asset, AssetExecutionContext, MonthlyPartitionsDefinition,
    Definitions, EnvVar, MaterializeResult, MetadataValue
)
import re

monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01")

DRIVER_URI_RE = re.compile(r"^gs://")

@asset(partitions_def=monthly_partitions)
def ccnews_html_text_month(context: AssetExecutionContext, dataproc: "DataprocJobRunner") -> MaterializeResult:
    """
    Monthly-partitioned asset: submits a Dataproc PySpark job that:
      - reads your monthly index (e.g. partitions_2025.parquet filtered to YYYY-MM)
      - downloads/processes WARC
      - writes parquet rows with html + extracted text to GCS
    """
    pk = context.partition_key
    year = pk[0:4]
    month = pk[5:7]

    index_uri = f"gs://gen-ai-tu/news/index/partitions_2025.parquet"

    out_root = "gs://gen-ai-tu/news/raw"

    main_py = "gs://gen-ai-tu/artifacts/ccnews_extract_job.py"
    pyfiles = [
        # e.g., a zipped package/wheel containing your code deps if you ship them this way
        # "gs://gen-ai-tu/artifacts/gen_ai_pipeline_deps.zip"
    ]

    args = [
        "--year", year,
        "--month", month,
        "--index-uri", index_uri,
        "--out-root", out_root,
        # optional tuning
        "--repartition", "100",
    ]

    job = dataproc.submit_pyspark(
        main_python_uri=main_py,
        pyfiles_uris=pyfiles,
        args=args,
    )

    driver_uri = getattr(job, "driver_output_resource_uri", None)

    md = {
        "year": year,
        "month": month,
        "dataproc_job_id": MetadataValue.text(job.reference.job_id),
    }
    if driver_uri and DRIVER_URI_RE.match(driver_uri):
        md["driver_output_uri"] = MetadataValue.url(driver_uri)

    # You can add the output prefix for this month as metadata for downstream assets
    md["output_prefix"] = MetadataValue.text(f"{out_root}/year={year}/month={month}")

    return MaterializeResult(metadata=md)