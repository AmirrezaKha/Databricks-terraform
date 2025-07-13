variable "databricks_host" {
  description = "Databricks workspace URL (e.g., https://<region>.azuredatabricks.net)"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}
