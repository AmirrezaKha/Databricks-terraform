terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.29.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# =============================
# Cluster
# =============================

resource "databricks_cluster" "etl_cluster" {
  cluster_name            = "etl-cluster"
  spark_version           = "14.3.x-scala2.12"
  node_type_id            = "Standard_DS3_v2"
  num_workers             = 2
  autotermination_minutes = 20
}

# =============================
# Upload Scripts to DBFS
# =============================

locals {
  domains = ["retail", "vix", "gas", "gold", "nasdaq"]
}

# Upload ETL scripts
resource "databricks_dbfs_file" "etl_scripts" {
  for_each = toset(local.domains)

  source = "${path.module}/../scripts/etl/${each.key}_etl.py"
  path   = "dbfs:/scripts/etl/${each.key}_etl.py"
}

# Upload anomaly detection scripts
resource "databricks_dbfs_file" "anomaly_scripts" {
  for_each = toset(local.domains)

  source = "${path.module}/../scripts/anomaly/${each.key}_anomaly.py"
  path   = "dbfs:/scripts/anomaly/${each.key}_anomaly.py"
}

# =============================
# Unity Catalog Setup
# =============================

resource "databricks_catalog" "main" {
  name    = "main"
  comment = "Default catalog for analytics"
}

resource "databricks_schema" "etl" {
  name         = "etl"
  catalog_name = databricks_catalog.main.name
  comment      = "ETL schema"
}

resource "databricks_schema" "ml" {
  name         = "ml"
  catalog_name = databricks_catalog.main.name
  comment      = "ML schema"
}

# =============================
# ETL Jobs
# =============================

resource "databricks_job" "etl_jobs" {
  for_each = toset(local.domains)

  name = "${each.key}-etl-job"

  existing_cluster_id = databricks_cluster.etl_cluster.id

  task {
    task_key = "${each.key}-etl-task"

    spark_python_task {
      python_file = databricks_dbfs_file.etl_scripts[each.key].path
    }
  }

  schedule {
    quartz_cron_expression = "0 ${10 + index(local.domains, each.key)} * * * ?"  # Staggered by hour
    timezone_id            = "UTC"
  }

  email_notifications {
    on_failure = ["your-email@example.com"]
  }

  max_concurrent_runs = 1
}

# =============================
# Anomaly Detection Jobs
# =============================

resource "databricks_job" "anomaly_jobs" {
  for_each = toset(local.domains)

  name = "${each.key}-anomaly-job"

  existing_cluster_id = databricks_cluster.etl_cluster.id

  task {
    task_key = "${each.key}-anomaly-task"

    spark_python_task {
      python_file = databricks_dbfs_file.anomaly_scripts[each.key].path
    }
  }

  schedule {
    quartz_cron_expression = "0 ${20 + index(local.domains, each.key)} * * * ?"  # Staggered later
    timezone_id            = "UTC"
  }

  email_notifications {
    on_failure = ["your-email@example.com"]
  }

  max_concurrent_runs = 1
}
