from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from pendulum import timezone

PROJECT_ID = "lobobranco-458901"
REGION = "us-central1"
CLUSTER_NAME = "lobobranco-dataproc"

CLUSTER_IMAGE_VERSION = "2.1-debian11"
SERVICE_ACCOUNT = "data-pipeline-sa@lobobranco-458901.iam.gserviceaccount.com"


default_args = {
    "start_date": datetime(2025, 6, 1, tzinfo=timezone("America/Sao_Paulo")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "data_proc_cluster",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["dataproc"]
) as dag:

    criar_cluster = DataprocCreateClusterOperator(
      task_id="criar_cluster",
      project_id=PROJECT_ID,
      region=REGION,
      cluster_name=CLUSTER_NAME,
      cluster_config={
          "master_config": {
              "num_instances": 1,
              "machine_type_uri": "n1-standard-2",
              "disk_config": {"boot_disk_size_gb": 50}
          },
          "worker_config": {
              "num_instances": 2,
              "machine_type_uri": "n1-standard-2",
              "disk_config": {"boot_disk_size_gb": 50}
          },
          "software_config": {
              "image_version": CLUSTER_IMAGE_VERSION,
          },
          "gce_cluster_config": {
              "service_account": SERVICE_ACCOUNT
          }
      }
)

    criar_cluster