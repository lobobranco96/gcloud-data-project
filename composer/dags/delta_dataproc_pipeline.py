from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from pendulum import timezone

# DATAPROC CLUSTER VARIABLES
PROJECT_ID = "lobobranco-458901"
REGION = "us-central1"
CLUSTER_NAME = "lobobranco-dataproc"
CLUSTER_IMAGE_VERSION = "2.1-debian11"
SERVICE_ACCOUNT = "data-pipeline-sa@lobobranco-458901.iam.gserviceaccount.com"


# DATALAKE PATH
BUCKET = "gs://lakehouse_lb_bucket"
RAW_PATH = f"{BUCKET}/lakehouse_data/raw"
BRONZE_PATH = f"{BUCKET}/lakehouse_data/bronze"
SILVER_PATH = f"{BUCKET}/lakehouse_data/silver"
GOLD_PATH = f"{BUCKET}/lakehouse_data/gold"

# SCRIPTS PATH
BRONZE_SCRIPT = "gs://lakehouse_lb_bucket/scripts/bronze.py"
SILVER_SCRIPT = "gs://lakehouse_lb_bucket/scripts/silver.py"
GOLD_SCRIPT = "gs://lakehouse_lb_bucket/scripts/gold.py"
EXPORT_SCRIPT = "gs://lakehouse_lb_bucket/scripts/gold_to_bigquery.py"


default_args = {
    "start_date": datetime(2025, 6, 11),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "delta_lake_pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dataproc", "delta"],
    start_date=datetime(2025, 6, 11, tzinfo=timezone("America/Sao_Paulo"))
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
                "service_account": SERVICE_ACCOUNT,
                "zone_uri": "us-central1-c"
            }
        }
  )

  executar_bronze = DataprocSubmitJobOperator(
      task_id="executar_bronze",
      project_id=PROJECT_ID,
      region=REGION,
      job={
          "placement": {"cluster_name": CLUSTER_NAME},
          "pyspark_job": {
              "main_python_file_uri": BRONZE_SCRIPT,
              "args": [
                  "--raw_path={{ params.raw_path }}/2025-06-11",#{{ ds }}",
                  "--bronze_path={{ params.bronze_path }}",
                  "--ingest_date=2025-06-11",#{{ ds }}",
              ],
          "jar_file_uris": [
              "gs://lakehouse_lb_bucket/jars/delta-core_2.12-2.3.0.jar",
              "gs://lakehouse_lb_bucket/jars/delta-storage-2.3.0.jar"
          ]
          },
      },
      params={
          "raw_path": RAW_PATH,
          "bronze_path": BRONZE_PATH,
      },
  )

  executar_silver = DataprocSubmitJobOperator(
      task_id="executar_silver",
      project_id=PROJECT_ID,
      region=REGION,
      job={
          "placement": {"cluster_name": CLUSTER_NAME},
          "pyspark_job": {
              "main_python_file_uri": SILVER_SCRIPT,
              "args": [
                  "--bronze_path={{ params.bronze_path }}",
                  "--silver_path={{ params.silver_path }}",
                  "--ingest_date={{ ds }}",
              ],
          "jar_file_uris": [
              "gs://lakehouse_lb_bucket/jars/delta-core_2.12-2.3.0.jar",
              "gs://lakehouse_lb_bucket/jars/delta-storage-2.3.0.jar"
          ]
          },
      },
      params={
          "bronze_path": BRONZE_PATH,
          "silver_path": SILVER_PATH,
      },
  )

  executar_gold = DataprocSubmitJobOperator(
      task_id="executar_gold",
      project_id=PROJECT_ID,
      region=REGION,
      job={
          "placement": {"cluster_name": CLUSTER_NAME},
          "pyspark_job": {
              "main_python_file_uri": GOLD_SCRIPT,
              "args": [
                  "--silver_path={{ params.silver_path }}",
                  "--gold_path={{ params.gold_path }}",
                  "--ingest_date={{ ds }}",
              ],
              "jar_file_uris": [
              "gs://lakehouse_lb_bucket/jars/delta-core_2.12-2.3.0.jar",
              "gs://lakehouse_lb_bucket/jars/delta-storage-2.3.0.jar"
          ]
          },
      },
      params={
          "silver_path": SILVER_PATH,
          "gold_path": GOLD_PATH,
      },
  )

  exportar_para_bigquery = DataprocSubmitJobOperator(
      task_id="exportar_para_bigquery",
      project_id=PROJECT_ID,
      region=REGION,
      job={
          "placement": {"cluster_name": CLUSTER_NAME},
          "pyspark_job": {
              "main_python_file_uri": EXPORT_SCRIPT,
              "args": [
                  "--gold_path={{ params.gold_path }}",
                  "--ingest_date={{ ds }}",
              ],
              "jar_file_uris": [
                  "gs://lakehouse_lb_bucket/jars/delta-core_2.12-2.3.0.jar",
                  "gs://lakehouse_lb_bucket/jars/delta-storage-2.3.0.jar",
                  "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar"
              ],
          },
      },
      params={
          "gold_path": GOLD_PATH,
      },
  )

  deletar_cluster = DataprocDeleteClusterOperator(
      task_id="deletar_cluster",
      project_id=PROJECT_ID,
      region=REGION,
      cluster_name=CLUSTER_NAME,
      trigger_rule=TriggerRule.ALL_DONE,
  )

  criar_cluster >> executar_bronze >> executar_silver >> executar_gold >> exportar_para_bigquery >> deletar_cluster
