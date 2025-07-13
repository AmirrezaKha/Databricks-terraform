# Databricks ETL & ML Platform (Delta Lake + Terraform + Unity Catalog)

## 🧩 Project Overview

This project ingests open-source CSV datasets from the internet, runs ETL jobs in Databricks using PySpark, stores results in Delta Lake, and applies ML anomaly detection. Unity Catalog and job automation are included.

## 📁 Domains Covered

| Domain     | ETL Script                    | Output Delta Table                  |
|------------|-------------------------------|--------------------------------------|
| Retail     | `retail_etl.py`               | `main.etl.retail_monthly_sales`      |
| VIX        | `vix_etl.py`                  | `main.etl.vix_weekly`                |
| Gas        | `gas_etl.py`                  | `main.etl.gas_prices`                |
| Gold       | `gold_etl.py`                 | `main.etl.gold_prices`               |
| Nasdaq     | `nasdaq_etl.py`               | `main.etl.nasdaq_history`            |

Each domain also has a corresponding anomaly detection script (`*_anomaly.py`) that writes results to `main.ml.<domain>_anomalies`.

---

## ⚙️ Infrastructure Overview

- **Provisioning**: Done with Terraform
- **Jobs**: Scheduled in Databricks (hourly, staggered)
- **Data Storage**: Delta Lake with Unity Catalog schemas (`main.etl`, `main.ml`)
- **Automation**: GitHub Actions CI/CD
- **Scripting**: PySpark for both ETL and ML tasks

---

## 🚀 Getting Started

### ✅ Requirements

- Databricks Workspace (with Unity Catalog enabled)
- Terraform >= 1.6.0
- GitHub CLI (for Actions)
- Databricks CLI
- Personal Access Token (PAT)

---

### 🧪 Job Scheduling Logic

Jobs are created dynamically for each domain using Terraform with different time slots:
    | Job Type     | Schedule (UTC)                    |
    |------------|-------------------------------|
    | ETL     | `retail_etl.py`               |
    | Anomaly ML        | `vix_etl.py`                  |

### 📬 CI/CD Pipeline

Terraform automatically exports:

- etl_job_ids → All ETL job IDs
- anomaly_job_ids → All anomaly detection job IDs
- retail_etl_job_id → Specific ETL job ID
- retail_anomaly_job_id → Specific ML job ID

### 🛠 Setup Instructions

```bash
# 1. Provision infrastructure
cd terraform/
terraform init
terraform apply -auto-approve

# 2. Upload scripts to workspace
databricks workspace import_dir scripts /Workspace/scripts

# 3. Run ETL scripts manually or as jobs

                    ┌────────────────────────────┐
                    │        GitHub Repo         │
                    │ (ETL Scripts + Terraform)  │
                    └────────────┬───────────────┘
                                 │ GitHub Actions CI/CD
            ┌────────────────────┼─────────────────────┐
            ▼                                            ▼
  ┌──────────────────────┐                   ┌──────────────────────┐
  │   Terraform Deploys  │                   │ Databricks CLI Upload│
  │  (Workspace, Jobs,   │                   │ (scripts, notebooks) │
  │   Clusters, Tables)  │                   └──────────────────────┘
  └────────────┬─────────┘
               │
               ▼
     ┌────────────────────┐
     │   Databricks Cloud │
     │  (Jobs, Delta Lake)│
     └────────────┬───────┘
                  ▼
        ┌────────────────────┐
        │ Unity Catalog /    │
        │ Delta Tables       │
        └────────────────────┘




