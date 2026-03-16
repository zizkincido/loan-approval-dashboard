# Loan Approval Data Pipeline

End-to-end batch data pipeline built on GCP.

## Architecture
Kaggle CSV → Kestra (orchestration) → GCS (data lake) → BigQuery raw → Spark (Dataproc) → BigQuery mart → Looker Studio

## Technologies
| Layer | Tool |
|---|---|
| Cloud | GCP |
| IaC | Terraform |
| Orchestration | Kestra |
| Data Lake | Google Cloud Storage |
| Processing | Apache Spark (Dataproc) |
| Data Warehouse | BigQuery |
| Dashboard | Looker Studio |

## Dashboard
[View live dashboard](https://lookerstudio.google.com/s/ntrayhQhIV8)

## How to run
1. `terraform apply` — provision GCP infrastructure
2. Upload Spark script to GCS
3. Trigger Kestra flow manually or wait for daily schedule (06:00 UTC)