variable "project_id" {
  description = "The GCP Project ID"
  type        = "STRING"
}

variable "region" {
  description = "GCP Region"
  type        = "STRING"
  default     = "europe-west3"
}

variable "dataset_id" {
  description = "The BigQuery Dataset ID"
  type        = "STRING"
  default     = "gold_analytics"
}
