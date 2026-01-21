from dagster import (
    asset, AssetExecutionContext, MonthlyPartitionsDefinition, get_dagster_logger, Config
)
from google.cloud.dataproc_v1 import Job, JobPlacement, PySparkJob, SubmitJobRequest
from google.cloud import dataproc_v1 as dataproc
import re
import os
from dagster_gcp.pipes import (
    PipesDataprocJobClient,
    PipesGCSContextInjector,
    PipesGCSMessageReader,
)
from typing import Optional
from gen_ai_pipeline.assets.etl.ccnews_preprocess import ccnews_preprocess

monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01")

DRIVER_URI_RE = re.compile(r"^gs://")

_CLUSTER_NAME_ALLOWED = re.compile(r"[^a-z0-9-]+")

def _make_cluster_name(partition_key: str) -> str:
    # partition_key like "2025-10-01" (monthly partitions can still look like this)
    base = f"news-chunking-{partition_key}".lower()
    base = _CLUSTER_NAME_ALLOWED.sub("-", base)
    base = base.strip("-")
    # Dataproc allows up to 51 chars
    return base[:51].rstrip("-")

class DataCollectionConfig(Config):
    year: int = 2025
    month: int = 10
    docs_uri: str = "gs://gen-ai-tu/news/clean/"
    repartition: int = 100
    out_root: str = "gs://gen-ai-tu/news"
    
@asset(partitions_def=monthly_partitions,
       deps=[ccnews_preprocess],
       group_name="etl",
       compute_kind="gcp",)
def ccnews_chunking_asset(
    context: AssetExecutionContext,
    config: DataCollectionConfig,
    dataproc_job_client: PipesDataprocJobClient,
):
    pk = context.partition_key
    # Extract year and month from partition key
    year = int(pk[0:4])
    month = int(pk[5:7])

    main_py = "gs://gen-ai-tu/artifacts/ccnews_chunking_job.py"
    pyfiles: list[str] = []
    project_id = os.getenv("GCP_PROJECT", "datascience-team-427407")    
    region = os.getenv("GCP_CLUSTER_REGION", "us-central1")
    subnetwork_uri = os.getenv("DATAPROC_SUBNETWORK_URI")
    zone_uri = os.getenv("DATAPROC_ZONE_URI")
    policy_uri = f"projects/{project_id}/regions/{region}/autoscalingPolicies/gen-ai-test-eu"    
    props = {
        # required for Pipes messages to work
        # "dataproc:pip.packages": "dagster-pipes,google-cloud-storage",
        "spark.pyspark.python": "/opt/gen-ai-env/env/bin/python",
        "spark.pyspark.driver.python": "/opt/gen-ai-env/env/bin/python",
        # make AWS creds available to driver + executors
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark:spark.executor.memory": "20g",
        "spark:spark.executor.cores": "4",
        "spark:spark.executor.memoryOverhead": "4g",
        "spark:spark.sql.shuffle.partitions": "48",
        "spark:spark.default.parallelism": "48",
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
            "initialization_actions": [{"executable_file": "gs://gen-ai-tu/artifacts/install_pixi_preprocess.sh",
                                        "execution_timeout": "1800s"}],
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-8",
                "disk_config": {"boot_disk_size_gb": 200},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-16",
                "disk_config": {"boot_disk_size_gb": 200},
            },
            "secondary_worker_config": {
                "num_instances": 4, # minimum is 2
                "machine_type_uri": "n1-standard-16",
                "disk_config": {"boot_disk_size_gb": 200},
                "is_preemptible": True, # True = Spot instances (Cheaper), False = Standard
            },
            "autoscaling_config": {
                "policy_uri": policy_uri
            }
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
                            properties=props,
                        ),
                    ),
                )
            },
            extras={
                "year": str(year),
                "month": str(month).zfill(2),
                "repartition": config.repartition,
                "out_root": config.out_root,
                "docs_uri": config.docs_uri,
            }
        )
        return job_run.get_materialize_result()
    except Exception as e:
        get_dagster_logger().error(f"Error during Dataproc job: {e}")
        raise  # Re-raise the exception to ensure it's properly handled by Dagster
    finally:
        if created:
            context.log.info(f"Deleting cluster: {cluster_name}")
            #cluster_client.delete_cluster(
            #    request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
            #)