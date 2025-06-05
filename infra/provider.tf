terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0"
    }
  }

  required_version = ">= 1.0"
}

provider "google" {
  credentials = file("${path.module}/credencial/lobobranco-458901-1258177c3d6e.json")
  project     = var.project_id
  region      = var.region
}