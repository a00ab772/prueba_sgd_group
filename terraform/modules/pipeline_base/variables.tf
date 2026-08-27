variable "project_name" {
  type    = string
  default = "data-pipeline"
}

variable "environment" {
  type = string
}

# --- Networking ---

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

# --- MWAA (Apache Airflow) ---

variable "mwaa_environment_class" {
  type    = string
  default = "mw1.small"
}

variable "mwaa_airflow_version" {
  type    = string
  default = "2.10.3"
}

# --- EMR (Spark) ---

variable "emr_release_label" {
  type    = string
  default = "emr-7.2.0"
}

variable "emr_master_instance_type" {
  type    = string
  default = "m5.xlarge"
}

variable "emr_core_instance_type" {
  type    = string
  default = "m5.xlarge"
}

variable "emr_core_instance_count" {
  type    = number
  default = 2
}

# --- Secrets Manager ---

variable "emr_credentials_secret_string" {
  type      = string
  default   = null
  sensitive = true
}