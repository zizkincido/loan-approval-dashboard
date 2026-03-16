terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# ── GCS Bucket (Data Lake) ──────────────────────────────────────────
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-loan-data-lake"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition { age = 30 }
    action    { type = "Delete" }
  }
}

# ── BigQuery Datasets ───────────────────────────────────────────────
resource "google_bigquery_dataset" "raw" {
  dataset_id = "loan_raw"
  location   = var.region
  description = "Raw loan data loaded from GCS"
}

resource "google_bigquery_dataset" "mart" {
  dataset_id = "loan_mart"
  location   = var.region
  description = "Transformed analytics-ready loan data"
}

# ── Dataproc Cluster (Spark) ────────────────────────────────────────
resource "google_dataproc_cluster" "spark_cluster" {
  name   = "loan-spark-cluster"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "e2-medium"
      disk_config {
        boot_disk_size_gb = 30
      }
    }
    worker_config {
      num_instances = 2
      machine_type  = "e2-medium"
      disk_config {
        boot_disk_size_gb = 30
      }
    }
    gce_cluster_config {
      service_account = "loan-pipeline-sa@${var.project_id}.iam.gserviceaccount.com"
    }
  }
}