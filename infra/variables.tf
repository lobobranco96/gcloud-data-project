variable "project_id" {
  description = "ID do projeto no GCP"
  type        = string
}

variable "region" {
  description = "Região dos recursos"
  type        = string
}
variable "cluster_name" {
  description = "Nome do Dataproc Cluster"
  type        = string
}

variable "service_account" {
  description = "Service account"
  type        = string
}

variable "location" {
  description = "Localização dos recursos"
  type        = string
}

variable "table_id" {
  description = "Nome da tabela"
  type        = string
}

variable "dataset_id" {
  description = "Dataset bigquery"
  type        = string
}