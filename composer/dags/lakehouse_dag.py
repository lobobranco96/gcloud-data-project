from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

PROJECT_ID = "lobobranco-458901"
REGION = "us-central1"
CLUSTER_NAME = "lobobranco-dataproc"

BUCKET = "gs://lakehouse_lb_bucket"
RAW_PATH = f"{BUCKET}/raw"
BRONZE_PATH = f"{BUCKET}/bronze"
SILVER_PATH = f"{BUCKET}/silver"
GOLD_PATH = f"{BUCKET}/gold"

BRONZE_SCRIPT = "gs://lakehouse_lb_bucket/scripts/bronze.py"
SILVER_SCRIPT = "gs://lakehouse_lb_bucket/scripts/silver.py"
GOLD_SCRIPT = "gs://lakehouse_lb_bucket/scripts/gold.py"

default_args = {
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "pipeline_lakehouse",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dataproc"]
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
                "properties": {
                    "spark:spark.jars": "gs://lakehouse_lb_bucket/jars/delta-core_2.12-2.2.0.jar",
                    "spark:spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                    "spark:spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                }
            },
            "gce_cluster_config": {
                "service_account": "data-pipeline-sa@lobobranco-458901.iam.gserviceaccount.com"
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
                    "--raw_path={{ params.raw_path }}/{{ ds }}",
                    "--bronze_path={{ params.bronze_path }}",
                    "--ingest_date={{ ds }}",
                ],
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
            },
        },
        params={
            "silver_path": SILVER_PATH,
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

    criar_cluster >> executar_bronze >> executar_silver >> executar_gold >> deletar_cluster
