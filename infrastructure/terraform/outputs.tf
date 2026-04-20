output "gcs_bucket_name" {
  description = "The name of the GCS bucket for Bronze data"
  value       = google_storage_bucket.bronze_lake.name
}

output "bigquery_dataset_id" {
  description = "The BigQuery Dataset ID"
  value       = google_bigquery_dataset.gold_analytics.dataset_id
}

output "service_account_email" {
  description = "The email of the pipeline service account"
  value       = google_service_account.pipeline_sa.email
}

output "instructions" {
  value = "Next step: Run 'gcloud iam service-accounts keys create ../../auth/service_account.json --iam-account=${google_service_account.pipeline_sa.email}' to generate your credential file."
}
