variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"
}

variable "credentials_file" {
  description = "Path to GCP service account key JSON"
  type        = string
}