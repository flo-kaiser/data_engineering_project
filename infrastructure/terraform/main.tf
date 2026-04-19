provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. GCS Bucket for Bronze Layer (Parquet Files)
resource "google_storage_bucket" "bronze_lake" {
  name          = "${var.project_id}-gold-bronze"
  location      = var.region
  force_destroy = true # Warning: Deletes all data on 'terraform destroy'

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# 2. BigQuery Dataset for Silver & Gold Layers
resource "google_bigquery_dataset" "gold_analytics" {
  dataset_id                  = var.dataset_id
  friendly_name               = "Gold Market Intelligence"
  description                 = "Contains Silver (Staging) and Gold (Marts) tables for Gold analysis."
  location                    = var.region
  delete_contents_on_destroy  = true
}

# 3. Service Account for the Pipeline (Ingestor & dbt)
resource "google_service_account" "pipeline_sa" {
  account_id   = "gold-pipeline-runner"
  display_name = "Service Account for Gold Intelligence Pipeline"
}

# 4. IAM Roles for Service Account
resource "google_project_iam_member" "bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
