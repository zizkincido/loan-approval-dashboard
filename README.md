# Loan Approval Data Pipeline

## Problem Description

Financial institutions process thousands of loan applications and need to
understand what drives approval decisions. This project builds a complete
batch data pipeline that ingests loan application data, processes it at
scale, and surfaces insights through an interactive dashboard.

**The pipeline answers two key questions:**
1. Does education level affect loan approval rates?
2. How does creditworthiness (CIBIL score) correlate with approval likelihood?

The dataset contains 4,269 loan applications with features including
applicant income, assets, credit score, education, and employment status.

By automating the ingestion, transformation, and visualization of this data,
the pipeline enables analysts to monitor approval trends without any manual
intervention — running on a daily schedule on Google Cloud Platform.

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
## BigQuery Optimization

### fact_loans
- **Partitioned by** `cibil_score` in 50-point buckets (300–900)
  → Dashboard queries filtering by credit band scan only relevant partitions
- **Clustered by** `education`, `loan_status`, `income_tier`
  → Aggregation queries group by these columns — clustering collocates
    matching rows, reducing bytes scanned by ~60-70%

### agg tables
- **Clustered by** their primary grouping columns
  → Looker Studio queries always filter/group by these — clustering
    makes each query faster and cheaper
## Dashboard
[View live dashboard](https://lookerstudio.google.com/embed/reporting/20547e89-9187-4f62-bdcd-66faf4599f6e/page/KgNsF)

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Python | 3.11+ | python.org |
| Terraform | 1.6+ | terraform.io |
| Docker Desktop | latest | docker.com |
| gcloud CLI | latest | cloud.google.com/sdk |
| Kaggle account | — | kaggle.com |

## How to reproduce

### 1. Clone the repo
git clone https://github.com/zizkincido/loan-approval-dashboard.git
cd loan-approval-dashboard

### 2. GCP setup
- Create a GCP project
- Enable APIs: BigQuery, GCS, Dataproc, IAM
- Create service account with roles: Storage Admin, BigQuery Admin, Dataproc Admin
- Download key as `loan-pipeline-key.json` (do NOT commit this)

### 3. Provision infrastructure
cd terraform
terraform init
terraform apply \
  -var="project_id=YOUR_PROJECT_ID" \
  -var="credentials_file=../loan-pipeline-key.json"
cd ..

### 4. Set up Kaggle credentials
- Download kaggle.json from kaggle.com/settings
- Place at: ~/.kaggle/kaggle.json (Linux/Mac) or C:\Users\YOU\.kaggle\kaggle.json (Windows)

### 5. Prepare data locally (one-time test)
pip install pandas pyarrow kaggle
python data/csv_to_parquet.py
gcloud storage cp data/loan_approval.parquet \
  gs://YOUR_PROJECT_ID-loan-data-lake/raw/loan_approval.parquet

### 6. Upload Spark script
gcloud storage cp spark/transform.py \
  gs://YOUR_PROJECT_ID-loan-data-lake/scripts/transform.py

### 7. Start Kestra
docker run --pull=always --rm -it -p 8080:8080 --user=root \
  -v "${PWD}/kestra-data:/app/storage" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  kestra/kestra:latest server local

### 8. Configure Kestra KV Pairs
Open http://localhost:8080 → Namespaces → default → KV Pairs
Add:
  GCP_SERVICE_ACCOUNT_KEY  → contents of loan-pipeline-key.json
  KAGGLE_USERNAME          → your Kaggle username
  KAGGLE_KEY               → your Kaggle API key

### 9. Deploy and run the flow
- Open http://localhost:8080 → Flows → Create
- Paste contents of kestra/flows/loan_pipeline.yml
- Click Save → Execute → Run now

### 10. Verify in BigQuery
SELECT loan_status, COUNT(*) as count
FROM `YOUR_PROJECT_ID.loan_mart.fact_loans`
GROUP BY loan_status;

### 11. View dashboard
[Looker Studio Dashboard](https://lookerstudio.google.com/embed/reporting/20547e89-9187-4f62-bdcd-66faf4599f6e/page/KgNsF)