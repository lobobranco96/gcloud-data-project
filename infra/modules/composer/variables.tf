variable "name" {}
variable "region" {}
variable "project_id" {}
variable "image_version" {
  description = "Versão da imagem do Composer"
  type        = string
}
variable "environment_size" {
  type        = string
}

variable "pypi_packages" {
  description = "Pacotes Python para instalar no Composer (equivalente a requirements.txt)"
  type        = map(string)
  default     = {}
}
variable "service_account" {
  description = "Conta de serviço"
  type        = string

}
