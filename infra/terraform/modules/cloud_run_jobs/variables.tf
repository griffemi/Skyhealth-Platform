variable "region" { default = "us-central1" }
variable "submitter_image" {}
variable "dbt_image" {}
variable "submitter_env" { type = map(string) default = {} }
variable "dbt_env" { type = map(string) default = {} }
