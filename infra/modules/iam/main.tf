resource "google_service_account" "data_pipeline_sa" {
  account_id   = "data-pipeline-sa"
  display_name = "Service Account for GCP Data Pipeline"
}

locals {
  roles = [
    "roles/dataproc.admin",          # Administra clusters e jobs
    "roles/dataproc.worker",         # Execução de jobs nos workers
    "roles/storage.admin",           # Acesso completo a buckets e objetos
    "roles/bigquery.admin",          # Acesso total a datasets e jobs
    "roles/composer.admin",          # Criação e gerenciamento de ambientes Composer
    "roles/composer.worker",         # Execução de DAGs no ambiente
    "roles/logging.logWriter",       # Gravação de logs
    "roles/monitoring.metricWriter", # Escrita de métricas no Cloud Monitoring
    "roles/iam.serviceAccountUser",   # Permite DAGs usarem outras service accounts
    "roles/compute.instanceAdmin.v1"
  ]
}

resource "google_project_iam_member" "iam_bindings" {
  for_each = toset(local.roles)

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.data_pipeline_sa.email}"
}
