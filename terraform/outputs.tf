# Output specific ETL job ID
output "retail_etl_job_id" {
  value = databricks_job.etl_jobs["retail"].id
}

# Output specific anomaly detection job ID
output "retail_anomaly_job_id" {
  value = databricks_job.anomaly_jobs["retail"].id
}

# Output all ETL job IDs
output "etl_job_ids" {
  value = {
    for domain, job in databricks_job.etl_jobs : domain => job.id
  }
}

# Output all Anomaly Detection job IDs
output "anomaly_job_ids" {
  value = {
    for domain, job in databricks_job.anomaly_jobs : domain => job.id
  }
}
