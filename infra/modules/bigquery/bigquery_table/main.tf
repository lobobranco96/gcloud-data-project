resource "google_bigquery_table" "this" {
  dataset_id = var.dataset_id
  table_id   = var.table_id
  deletion_protection = false
}