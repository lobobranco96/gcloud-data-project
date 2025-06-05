#####################
# Iam Roles
#####################

module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id
}

#####################
# Buckets GCS
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