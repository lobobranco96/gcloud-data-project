#####################
# Módulo Iam Roles
#####################

module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id
}

#####################
# Módulo Buckets GCS
#####################

# Bucket para o lakehouse e scripts
module "lakehouse_lb_bucket" {
  source        = "./modules/gcs"
  name          = "lakehouse_lb_bucket"
  location      = var.location
  force_destroy = true
}



#############################
# Modulo Cloud Composer
#############################

# Provisiona ambiente gerenciado do Airflow com dependências necessárias
module "composer_env" {
  source           = "./modules/composer"
  name             = "lobobranco-composer"
  region           = var.region     
  project_id       = var.project_id
  image_version    = var.image_version  
  environment_size = var.environment_size
  service_account = var.service_account
  

  pypi_packages = {
  "apache-airflow-providers-google" = ">=10.0.0"     
  "google-cloud-dataproc"           = ">=5.0.0"      
  "google-cloud-storage"            = ">=2.0.0"      
  "pyspark"                         = ">=3.3.0"      
}
}

data "google_project" "project" {
  project_id = var.project_id
}