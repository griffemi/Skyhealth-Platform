variable "project_id" {}
variable "region" { default = "us-central1" }
variable "zone" { default = "us-central1-a" }
variable "bucket_name" { default = "skyfeed-lake" }
variable "dataset_name" { default = "skyfeed_gold" }

variable "submitter_image" { default = "" }
variable "dbt_image" { default = "" }
variable "streamlit_image" { default = "" }
variable "lightdash_image" { default = "" }
variable "parquet_export" { default = "" }
