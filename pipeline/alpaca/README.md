# Alpaca Pipeline

ETL pipeline that pulls historical and live market data from the Alpaca Markets API for ~60 tracked tech stocks, cleans and validates it, loads it into PostgreSQL RDS, and forwards records to the RAG ingest service.

## Overview

The pipeline runs as an **AWS Lambda** function triggered every 20 minutes during market hours (Mon–Fri, 12:00–22:00 UTC) via EventBridge Scheduler.

Each run executes three steps:

1. **Extract** (`alpaca_extract.py`) — fetches daily OHLCV bars (history from 2024-01-01 to today) and the latest live bar for each ticker from the Alpaca v2 API.
2. **Transform** (`alpaca_transform_cleaning.py`) — converts data types, drops rows with missing critical fields, validates numeric ranges, and deduplicates.
3. **Load** (`alpaca_load.py`) — upserts cleaned data into the `alpaca_history` and `alpaca_live` tables in RDS via `psycopg2`.

After extraction, records are also sent in batches to the RAG ingest Lambda (`rag_ingest_invoke.py`) so the dashboard chatbot has up-to-date market context.

### Key Files

| File | Purpose |
|---|---|
| `run_pipeline.py` | Orchestrator — runs extract → transform → load. Exposes `lambda_handler` for AWS and `__main__` for local. |
| `alpaca_extract.py` | Calls Alpaca bars and latest-bars endpoints. |
| `alpaca_transform_cleaning.py` | Data cleaning, type conversion, validation. |
| `alpaca_load.py` | RDS upsert logic for history and live tables. |
| `config.py` | Retrieves credentials from AWS Secrets Manager. |
| `top_100_tech_companies.py` | Ticker-to-company-name mapping used across all pipelines. |
| `rag_ingest_invoke.py` | Invokes the RAG ingest Lambda with extracted data. |
| `logger.py` | Shared logging config. |

## What You Need

### Credentials

All credentials are stored in **AWS Secrets Manager** under `c22-trade-research-tool-secrets`. The secret must contain:

| Key | Purpose |
|---|---|
| `ALPACA_API_KEY` | Alpaca API key ID |
| `ALPACA_API_SECRET` | Alpaca API secret key |
| `host` | RDS endpoint |
| `port` | RDS port (default `5432`) |
| `dbname` | Database name |
| `username` | Database user |
| `password` | Database password |

### Dependencies

```
alpaca-py
pandas
requests
python-dotenv
pytz
numpy
psycopg2-binary
boto3
```

Install with:

```bash
pip install -r requirements.txt
```

### Prerequisites

- RDS database with schema initialised (`rds_schema/schema.sql`)
- AWS credentials configured locally (`aws configure`) or via IAM role
- Alpaca Markets account with API keys

## How to Deploy

Infrastructure is managed in `terraform/pipeline/`. The deploy script pushes the Docker image to ECR.

### 1. Provision infrastructure (first time only)

```bash
cd terraform/pipeline
terraform init
terraform apply
```

This creates the ECR repository, Lambda function, IAM roles, and EventBridge schedule.

### 2. Build and push the Docker image

```bash
cd pipeline/alpaca
bash deploy_alpaca_pipeline.sh
```

The script authenticates with ECR, builds a `linux/amd64` Lambda container image, and pushes it as `latest`.

### 3. Update the Lambda to use the new image

```bash
aws lambda update-function-code \
  --function-name c22-stocksiphon-alpaca-lambda \
  --image-uri <ECR_URI>:latest
```

## How to Run

### Locally

```bash
cd pipeline/alpaca
python run_pipeline.py
```

Requires AWS credentials in environment (for Secrets Manager access) and network access to RDS.

### Invoke the Lambda manually

```bash
aws lambda invoke \
  --function-name c22-stocksiphon-alpaca-lambda \
  --payload '{}' \
  response.json

cat response.json
```

### Run tests

```bash
cd pipeline/alpaca
pytest test_alpaca_extract.py test_alpaca_transform_cleaning.py test_alpaca_load.py -v
```

## Notes

- **Schedule** — EventBridge triggers the Lambda every 20 minutes on weekdays between 12:00–22:00 UTC, covering US market hours plus pre/post-market.
- **Ticker universe** — defined in `top_100_tech_companies.py` (~60 tickers). This file is shared across all pipelines. Adding a ticker here automatically includes it in the next run.
- **SSL** — `global-bundle.pem` is bundled in the Docker image for RDS SSL connections.
- **Lambda timeout** — set to 600 seconds (10 minutes) to handle the full extraction across all tickers.
- **RAG batching** — extracted records are sent to the RAG ingest Lambda in batches of 20 to avoid payload size limits.
