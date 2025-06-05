from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime

CLUSTER_NAME = "lobobranco-dataproc"
PROJECT_ID = "lobobranco-458901"
REGION = "us-central1"

def load_to_bigquery():
    client = bigquery.Client()
    dataset_id = "dataset_project"
    table_id = "produtos"
    uri = "gs://processed/*.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    load_job = client.load_table_from_uri(
        uri, f"{dataset_id}.{table_id}", job_config=job_config
    )
    load_job.result()
    print("Dados carregados com sucesso no BigQuery.")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='pipeline_gcp_completo',
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    criar_cluster = DataprocCreateClusterOperator(
      task_id="criar_cluster",
      project_id="lobobranco-458901",
      region="us-central1",
      cluster_name="lobobranco-dataproc",
      cluster_config={
          "master_config": {
              "num_instances": 1,
              "machine_type_uri": "n1-standard-2",
              "disk_config": {
                  "boot_disk_size_gb": 50
              }
          },
          "worker_config": {
              "num_instances": 1,
              "machine_type_uri": "n1-standard-2",
              "disk_config": {
                  "boot_disk_size_gb": 50
              }
          },
          "gce_cluster_config": {
              "service_account": "data-pipeline-sa@lobobranco-458901.iam.gserviceaccount.com"
          }
      }
    )

    executar_spark = DataprocSubmitJobOperator(
        task_id="executar_spark",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://scripts_bucket_lb/transformation.py"
            },
        }
    )

    carregar_bigquery = PythonOperator(
        task_id="carregar_bigquery",
        python_callable=load_to_bigquery
    )

    deletar_cluster = DataprocDeleteClusterOperator(
        task_id="deletar_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done" 
    )

    criar_cluster >> executar_spark >> carregar_bigquery >> deletar_cluster
