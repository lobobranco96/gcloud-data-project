#####################
# Módulo Iam Roles
#####################

module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id
}
