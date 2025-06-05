resource "google_service_account" "data_pipeline_sa" {
  account_id   = "data-pipeline-sa"
  display_name = "Service Account for GCP Data Pipeline"
}

# Permissões essenciais
locals {
  roles = [
    "roles/compute.instanceAdmin.v1",
    "roles/dataproc.worker",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/storage.objectAdmin",
    "roles/composer.admin",
    "roles/bigquery.admin",
    "roles/composer.worker",
    "roles/iam.serviceAccountUser",
    "roles/storage.admin"
  ]
}

resource "google_project_iam_member" "iam_bindings" {
  for_each = toset(local.roles)

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.data_pipeline_sa.email}"
}
