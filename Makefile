.PHONY: help setup download convert upload-gcs upload-spark test lint infra-init infra-apply kestra-start pipeline-run

# ── Variables ────────────────────────────────────────────────────────
PROJECT_ID  ?= YOUR_PROJECT_ID
REGION      ?= europe-west1
BUCKET      = $(PROJECT_ID)-loan-data-lake
CREDS       ?= ../loan-pipeline-key.json

help:
	@echo ""
	@echo "  Loan Approval Pipeline — available commands"
	@echo ""
	@echo "  Setup"
	@echo "    make setup           Install all Python dependencies"
	@echo ""
	@echo "  Data"
	@echo "    make download        Download dataset from Kaggle"
	@echo "    make convert         Convert CSV to Parquet"
	@echo "    make upload-gcs      Upload Parquet to GCS data lake"
	@echo "    make upload-spark    Upload Spark script to GCS"
	@echo ""
	@echo "  Infrastructure"
	@echo "    make infra-init      terraform init"
	@echo "    make infra-apply     terraform apply"
	@echo "    make infra-destroy   terraform destroy"
	@echo ""
	@echo "  Orchestration"
	@echo "    make kestra-start    Start Kestra locally via Docker"
	@echo ""
	@echo "  Quality"
	@echo "    make test            Run pytest"
	@echo "    make lint            Run flake8 linter"
	@echo ""
	@echo "  All-in-one"
	@echo "    make all             setup + download + convert + upload-gcs + upload-spark"
	@echo ""

# ── Setup ────────────────────────────────────────────────────────────
setup:
	pip install -r requirements.txt

# ── Data ─────────────────────────────────────────────────────────────
download:
	kaggle datasets download -d rohitgrewal/loan-approval-dataset \
		-p data/ --unzip

convert:
	python data/csv_to_parquet.py

upload-gcs:
	gcloud storage cp data/loan_approval.parquet \
		gs://$(BUCKET)/raw/loan_approval.parquet

upload-spark:
	gcloud storage cp spark/transform.py \
		gs://$(BUCKET)/scripts/transform.py

# ── Infrastructure ───────────────────────────────────────────────────
infra-init:
	cd terraform && terraform init

infra-apply:
	cd terraform && terraform apply \
		-var="project_id=$(PROJECT_ID)" \
		-var="credentials_file=$(CREDS)"

infra-destroy:
	cd terraform && terraform destroy \
		-var="project_id=$(PROJECT_ID)" \
		-var="credentials_file=$(CREDS)"

# ── Orchestration ────────────────────────────────────────────────────
kestra-start:
	docker run --pull=always --rm -it -p 8080:8080 --user=root \
		-v "$(PWD)/kestra-data:/app/storage" \
		-v /var/run/docker.sock:/var/run/docker.sock \
		kestra/kestra:latest server local

# ── Quality ──────────────────────────────────────────────────────────
test:
	pytest tests/ -v

lint:
	flake8 spark/ data/ tests/ --max-line-length=100

# ── All-in-one ───────────────────────────────────────────────────────
all: setup download convert upload-gcs upload-spark