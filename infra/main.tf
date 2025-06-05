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

# Bucket para dados brutos (raw)
module "raw_bucket" {
  source        = "./modules/gcs"
  name          = "raw_bucket_lb"
  location      = var.location
  force_destroy = true
}

# Bucket para dados transformados (processed)
module "processed_bucket" {
  source        = "./modules/gcs"
  name          = "processed_bucket_lb"
  location      = var.location
  force_destroy = true
}

# Bucket para pyspark scripts
module "scripts_bucket" {
  source        = "./modules/gcs"
  name          = "scripts_bucket_lb"
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