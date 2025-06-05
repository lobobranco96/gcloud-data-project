resource "google_dataproc_cluster" "default" {
  name   = var.cluster_name
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
      disk_config {
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
      disk_config {
        boot_disk_size_gb = 50
      }
    }

    gce_cluster_config {
      service_account = var.service_account
    }
  }

  labels = {
    env = var.environment
  }
}
