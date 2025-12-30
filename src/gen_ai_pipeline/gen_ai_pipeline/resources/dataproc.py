from dataclasses import dataclass
from google.cloud import dataproc_v1
from google.api_core.client_options import ClientOptions

@dataclass
class DataprocConfig:
    project_id: str
    region: str
    cluster_name: str
    staging_bucket: str  # gs://... where main + pyfiles live

class DataprocJobRunner:
    def __init__(self, cfg: DataprocConfig):
        self._cfg = cfg
        endpoint = f"{cfg.region}-dataproc.googleapis.com:443"
        self._client = dataproc_v1.JobControllerClient(
            client_options=ClientOptions(api_endpoint=endpoint)
        )

    @property
    def cfg(self) -> DataprocConfig:
        return self._cfg

    def submit_pyspark(self, *, main_python_uri: str, pyfiles_uris: list[str], args: list[str]) -> dataproc_v1.Job:
        job = {
            "placement": {"cluster_name": self._cfg.cluster_name},
            "pyspark_job": {
                "main_python_file_uri": main_python_uri,
                "python_file_uris": pyfiles_uris,
                "args": args,
            },
        }
        op = self._client.submit_job_as_operation(
            request={
                "project_id": self._cfg.project_id,
                "region": self._cfg.region,
                "job": job,
            }
        )
        return op.result()