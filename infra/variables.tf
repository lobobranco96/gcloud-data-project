variable "project_id" {
  description = "ID do projeto no GCP"
  type        = string
}

variable "region" {
  description = "Região dos recursos"
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

variable "image_version" {
  description = "Versão da imagem do Cloud Composer"
  type        = string
}

variable "environment_size" {
  description = "Tamanho do ambiente do Composer"
  type        = string
}