locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "de-divvy-project"
}

variable "region" {
  description = "Region for GCP resources. Check location options: https://cloud.google.com/about/locations"
  default = "asia-south1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for data lake bucket."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "f1_dataset"
}