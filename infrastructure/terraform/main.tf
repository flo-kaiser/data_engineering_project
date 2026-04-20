provider "google" {
  project = var.project_id
  region  = var.region
}

# --- 0. API Activation (Reproducibility) ---
# Automatically enables all required Google Cloud Services
locals {
  services = [
    "iam.googleapis.com",
    "storage-api.googleapis.com",
    "bigquery.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com"
  ]
}

resource "google_project_service" "enabled_services" {
  for_each = toset(local.services)
  project  = var.project_id
  service  = each.key

  disable_on_destroy = false
}

# --- 1. GCS Bucket for Bronze Layer (Parquet Files) ---
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

  depends_on = [google_project_service.enabled_services]
}

# --- 2. BigQuery Dataset for Silver & Gold Layers ---
resource "google_bigquery_dataset" "gold_analytics" {
  dataset_id                  = var.dataset_id
  friendly_name               = "Gold Market Intelligence"
  description                 = "Contains Silver (Staging) and Gold (Marts) tables for Gold analysis."
  location                    = var.region
  delete_contents_on_destroy  = true

  depends_on = [google_project_service.enabled_services]
}

# --- 3. Service Account for the Pipeline (Ingestor & dbt) ---
resource "google_service_account" "pipeline_sa" {
  account_id   = "gold-pipeline-runner"
  display_name = "Service Account for Gold Intelligence Pipeline"

  depends_on = [google_project_service.enabled_services]
}

# --- 4. IAM Roles for Service Account ---
# Granting necessary permissions for BigQuery, Storage and Service Account management
locals {
  pipeline_roles = [
    "roles/bigquery.admin",
    "roles/storage.admin",
    "roles/iam.serviceAccountUser"
  ]
}

resource "google_project_iam_member" "pipeline_roles" {
  for_each = toset(local.pipeline_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
