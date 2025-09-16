variable "region" {
  description = "Region for the primary subnetwork."
  type        = string
}

variable "env" {
  description = "Environment slug for naming."
  type        = string
}

variable "subnet_cidr" {
  description = "CIDR block for the primary subnetwork."
  type        = string
  default     = "10.20.0.0/20"
}
