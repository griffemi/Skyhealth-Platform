variable "project_id" {
  description = "GCP project id."
  type        = string
}

variable "env" {
  description = "Deployment environment slug (dev|stg|prod)."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "Primary region for regional resources."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Default compute zone for zonal resources."
  type        = string
  default     = "us-central1-a"
}

variable "dagster_machine_type" {
  description = "Machine type for the Dagster control plane VM."
  type        = string
  default     = "e2-standard-2"
}

variable "dagster_disk_size_gb" {
  description = "Boot disk size for the Dagster VM."
  type        = number
  default     = 100
}

variable "dagster_boot_image" {
  description = "Source image for Dagster VM boot disk."
  type        = string
  default     = "projects/debian-cloud/global/images/family/debian-12"
}

variable "streamlit_image" {
  description = "Container image for the Streamlit Cloud Run service."
  type        = string
  default     = ""
  validation {
    condition     = var.streamlit_image != ""
    error_message = "Set streamlit_image to a container image URI before deploying."
  }
}

variable "budget_amount" {
  description = "Monthly budget alert threshold in USD."
  type        = number
  default     = 200
}

variable "notification_channels" {
  description = "Monitoring notification channel resource names for alerting."
  type        = list(string)
  default     = []
}

variable "billing_account" {
  description = "Billing account id (XXXXXX-XXXXXX-XXXXXX) for budget alerts. Leave blank to skip budget creation."
  type        = string
  default     = ""
}

variable "bronze_retention_days" {
  description = "Retention in days before Bronze data moves to colder storage."
  type        = number
  default     = 60
}

variable "silver_retention_days" {
  description = "Retention in days before Silver data moves to colder storage."
  type        = number
  default     = 120
}

variable "gold_retention_days" {
  description = "Retention in days before Gold data moves to colder storage."
  type        = number
  default     = 365
}

variable "dataproc_staging_bucket" {
  description = "Optional existing bucket for Dataproc staging. Leave blank to manage automatically."
  type        = string
  default     = ""
}

variable "streamlit_max_instances" {
  description = "Max autoscaled instances for Streamlit Cloud Run service."
  type        = number
  default     = 1
}

variable "streamlit_cpu" {
  description = "vCPU allocation for Streamlit Cloud Run service."
  type        = number
  default     = 1
}

variable "streamlit_memory" {
  description = "Memory allocation for Streamlit Cloud Run service (in Mi)."
  type        = number
  default     = 1024
}

variable "dagster_sql_tier" {
  description = "Cloud SQL machine tier for Dagster metadata."
  type        = string
  default     = "db-custom-1-3840"
}

variable "dagster_sql_disk_size" {
  description = "Cloud SQL storage size in GB."
  type        = number
  default     = 50
}

variable "dagster_sql_backup_retention" {
  description = "Number of retained automated backups for Dagster metadata."
  type        = number
  default     = 7
}

variable "project_services" {
  description = "Additional project services to enable."
  type        = list(string)
  default     = []
}
