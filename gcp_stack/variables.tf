variable "credentials" {
  description = "My Credentials"
  default     = "../creds/de-dbt.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  #Update the below to your desired project
  default = "de-zoomcamp-412103"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "australia-southeast1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "australia-southeast1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "trips_data_all"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "dbt-data-lake"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}