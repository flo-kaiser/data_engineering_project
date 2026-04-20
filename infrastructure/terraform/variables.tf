variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-west3"
}

variable "dataset_id" {
  description = "The BigQuery Dataset ID"
  type        = string
  default     = "gold_analytics"
}
