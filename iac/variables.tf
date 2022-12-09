locals {
  project_id             = replace(var.project_id, "-", "_")
  data_lake_bucket       = "${local.project_id}_data-lake"
  bigquery_dataset       = "${local.project_id}_all_data"
}

variable "project_name" {
  description = "Your GCP Project Name"
  default     = "tpch-dbgen"
  type        = string
}

variable "project_id" {
  description = "Your GCP Project ID"
  default     = "tpch-dbgen-367914"
  type        = string
}

variable "state_bucket" {
  description = "Bucket name for storing terrafrom state and lock files"
  default     = "tpch_terraform_state"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-west4"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
  type        = string
}