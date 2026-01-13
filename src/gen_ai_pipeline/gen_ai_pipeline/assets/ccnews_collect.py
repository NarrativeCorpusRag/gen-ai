from dagster import (
    asset, AssetExecutionContext, MonthlyPartitionsDefinition, get_dagster_logger
)
from dagster_gcp.pipes import PipesDataprocJobClient
from google.cloud.dataproc_v1 import Job, JobPlacement, PySparkJob, SubmitJobRequest
from google.cloud import dataproc_v1 as dataproc
import re
import os
from dagster_gcp.pipes import PipesDataprocJobClient

monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01")

DRIVER_URI_RE = re.compile(r"^gs://")

_CLUSTER_NAME_ALLOWED = re.compile(r"[^a-z0-9-]+")

def _make_cluster_name(partition_key: str) -> str:
    # partition_key like "2025-10-01" (monthly partitions can still look like this)
    base = f"news-collect-{partition_key}".lower()
    base = _CLUSTER_NAME_ALLOWED.sub("-", base)
    base = base.strip("-")
    # Dataproc allows up to 51 chars
    return base[:51].rstrip("-")

@asset(partitions_def=monthly_partitions)
def ccnews_html_text_month(
    context: AssetExecutionContext,
    dataproc_job_client: PipesDataprocJobClient,
):
    pk = context.partition_key
    year = pk[0:4]
    month = pk[5:7]


    index_uri = "gs://gen-ai-tu/index/"
    out_root = "gs://gen-ai-tu/news/raw"

    main_py = "gs://gen-ai-tu/artifacts/ccnews_extract_job.py"
    pyfiles: list[str] = []
    aws_key = os.getenv("ASCII_AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("ASCII_AWS_SECRET_ACCESS_KEY")
    project_id = os.getenv("GCP_PROJECT", "datascience-team-427407")    
    region = os.getenv("GCP_CLUSTER_REGION", "us-central1")
    subnetwork_uri = os.getenv("DATAPROC_SUBNETWORK_URI")
    zone_uri = os.getenv("DATAPROC_ZONE_URI")
    
    
    props = {
        # required for Pipes messages to work
        # "dataproc:pip.packages": "dagster-pipes,google-cloud-storage",
        # required for env
        "spark.pyspark.python": "/opt/gen-ai-env/env/bin/python",
        "spark.pyspark.driver.python": "/opt/gen-ai-env/env/bin/python",
        # make AWS creds available to driver + executors
        "spark.yarn.appMasterEnv.ASCII_AWS_ACCESS_KEY_ID": aws_key,
        "spark.yarn.appMasterEnv.ASCII_AWS_SECRET_ACCESS_KEY": aws_secret,
        "spark.executorEnv.ASCII_AWS_ACCESS_KEY_ID": aws_key,
        "spark.executorEnv.ASCII_AWS_SECRET_ACCESS_KEY": aws_secret,
    }
    # Create the cluster client.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )
    run_suffix = context.run_id[:8]
    cluster_name = _make_cluster_name(f"{pk}-{run_suffix}")
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    gce_cluster_config = {"subnetwork_uri": subnetwork_uri, "tags": ["dataproc-cluster"]}
    if zone_uri:
        gce_cluster_config["zone_uri"] = zone_uri

    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "gce_cluster_config": gce_cluster_config,
            "initialization_actions": [{"executable_file": "gs://gen-ai-tu/artifacts/install_pixi_env.sh"}],
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_size_gb": 100},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_size_gb": 100},
            },
        },
    }

    created = False
    try:
        op = cluster_client.create_cluster(
            request={"project_id": project_id, "region": region, "cluster": cluster}
        )
        op.result()
        created = True
        context.log.info(f"Cluster created: {cluster_name}")

        job_run = dataproc_job_client.run(
            context=context,
            submit_job_params={
                "request": SubmitJobRequest(
                    region=region,
                    project_id=project_id,
                    job=Job(
                        placement=JobPlacement(cluster_name=cluster_name),
                        pyspark_job=PySparkJob(
                            main_python_file_uri=main_py,
                            args=[
                                "--year", year,
                                "--month", month,
                                "--index-uri", index_uri,
                                "--out-root", out_root,
                                "--repartition", "100",
                            ],
                            properties=props,
                        ),
                    ),
                )
            },
        )
        return job_run.get_results()
    except Exception as e:
        get_dagster_logger().error(f"Error during Dataproc job: {e}")
    finally:
        if created:
            context.log.info(f"Deleting cluster: {cluster_name}")
            cluster_client.delete_cluster(
                request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
            )