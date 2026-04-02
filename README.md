# Stock Siphon — Tech Stock Research Tool

## Overview

A cloud-native platform that aggregates market data, news, and social sentiment for ~40 tracked tech stocks, then surfaces it through an interactive Streamlit dashboard with an AI-powered chatbot.

The system consists of four main components:

1. **ETL Pipelines** — Three independent pipelines (Alpaca market data, RSS news feeds, Reddit posts) extract data on a schedule, enrich it with OpenAI relevance/sentiment scoring, and load it into a shared PostgreSQL database.
2. **RAG Service** — A retrieval-augmented generation service backed by ChromaDB that ingests pipeline output and answers natural-language queries with source citations.
3. **Dashboard** — A Streamlit application with two tabs (Market Data trends and Search Company deep-dives) plus a floating AI chatbot sidebar.
4. **Infrastructure** — Terraform modules for RDS, Lambda, ECR, ECS, EventBridge, API Gateway, EFS, and CloudWatch.

### Architecture

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  Alpaca API  │   │  RSS Feeds   │   │  Reddit API  │
└──────┬───── ┘   └──────┬──────┘   └──────┬──────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ Alpaca       │   │ RSS          │   │ Reddit       │
│ Lambda       │   │ Lambda       │   │ Lambda       │
└──────┬──────┘   └──────┬──────┘   └──────┬──────┘
       │                  │                  │
       ├──────────────────┼──────────────────┤
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────────────────────────────────────────┐
│               PostgreSQL RDS                     │
└──────────────────────┬──────────────────────────┘
                       │
       ┌───────────────┼───────────────┐
       ▼                               ▼
┌─────────────┐                 ┌─────────────┐
│ RAG Ingest   │                 │  Dashboard   │
│ Lambda       │                 │  (ECS)       │
└──────┬──────┘                 └──────┬──────┘
       ▼                               │
┌─────────────┐                        │
│ ChromaDB     │                        │
│ (ECS + EFS)  │◄───── RAG Query ──────┘
└─────────────┘        Lambda
                         ▲
                         │
                    API Gateway
```

### Repository structure

```
├── dashboard/                  # Streamlit dashboard (ECS Fargate)
│   ├── summary/                #   "Search Company" tab
│   ├── trends/                 #   "Market Data" tab
│   ├── chatbot/                #   AI chatbot sidebar
│   └── README.md
├── pipeline/
│   ├── alpaca/                 # Alpaca market data pipeline (Lambda)
│   │   └── README.md
│   ├── rss/                    # RSS news feed pipeline (Lambda)
│   │   └── README.md
│   └── reddit/                 # Reddit social sentiment pipeline (Lambda)
│       └── README.md
├── rag_service/                # RAG ingest + query Lambdas + ChromaDB
│   ├── app/                    #   Embed, ingest, retrieve, query modules
│   └── tests/
├── rds_schema/                 # Database schema and seed script
│   └── schema.sql
├── terraform/
│   ├── database/               #   RDS instance
│   ├── pipeline/               #   Lambda + ECR + EventBridge for all 3 pipelines
│   ├── dashboard/              #   ECS cluster + task for Streamlit
│   ├── rag/                    #   RAG Lambdas + ChromaDB ECS + EFS + API Gateway
│   ├── secrets_repository/     #   Secrets Manager
│   └── state_bucket/           #   S3 remote state
└── README.md
```

## What You Need

### AWS Secrets Manager

Every service reads credentials from a single secret named `c22-trade-research-tool-secrets` in `eu-west-2`. The secret must contain:

| Key | Used by |
|---|---|
| `OPENAI_API_KEY` | RSS pipeline, Reddit pipeline, RAG service |
| `ALPACA_API_KEY` | Alpaca pipeline |
| `ALPACA_API_SECRET` | Alpaca pipeline |
| `host` | All services (RDS host) |
| `port` | All services (RDS port, default `5432`) |
| `dbname` | All services (database name) |
| `username` | All services (database user) |
| `password` | All services (database password) |

Create the secret:

```bash
aws secretsmanager create-secret \
  --name "c22-trade-research-tool-secrets" \
  --secret-string '{
    "host": "...",
    "port": 5432,
    "dbname": "...",
    "username": "...",
    "password": "...",
    "OPENAI_API_KEY": "sk-...",
    "ALPACA_API_KEY": "...",
    "ALPACA_API_SECRET": "..."
  }' \
  --region eu-west-2
```

### Container environment variables

These are set automatically by Terraform on the deployed containers:

| Variable | Service | Purpose |
|---|---|---|
| `SECRETS_REPO_NAME` | Dashboard (ECS) | Secrets Manager secret name |
| `RAG_API_URL` | Dashboard (ECS) | API Gateway URL for RAG queries |
| `SECRET_NAME` | RAG Lambdas | Secrets Manager secret name |
| `CHROMA_HOST` | RAG Lambdas | ChromaDB ECS service hostname |

### Local development

For running services locally, set these environment variables (or use a `.env` file):

```bash
export DB_HOST="..."
export DB_NAME="..."
export DB_USER="..."
export DB_PASSWORD="..."
export DB_PORT="5432"
export OPENAI_API_KEY="sk-..."
export ALPACA_API_KEY="..."
export ALPACA_API_SECRET="..."
export RAG_API_URL="https://..."
```

### Prerequisites

- AWS CLI configured (`aws configure`) with permissions for ECR, ECS, Lambda, Secrets Manager, RDS, S3, EventBridge, API Gateway, EFS.
- Docker installed (for building container images).
- Terraform installed (for infrastructure provisioning).
- Python 3.12+ (Lambda images use 3.12; dashboard uses 3.13).

## How to Deploy

Deployment follows this order: state bucket → database → secrets → pipelines → RAG service → dashboard.

### 1. Create the Terraform state bucket

```bash
cd terraform/state_bucket
terraform init
terraform apply
```

Creates the S3 bucket `c22-tsrt-terraform-state` for remote Terraform state.

### 2. Provision the database

```bash
cd terraform/database
terraform init
terraform apply
```

Creates a PostgreSQL 16 RDS instance (`db.t3.micro`, 20 GB, publicly accessible).

### 3. Initialise the schema

```bash
cd rds_schema
python create_rds.py
```

Runs `schema.sql` against RDS, which creates all tables and seeds the `stock` table with 40 tech tickers.

### 4. Create the secrets

```bash
cd terraform/secrets_repository
terraform init
terraform apply
```

Then populate the secret via the AWS CLI command shown above.

### 5. Deploy the pipelines

Provision Lambda functions, ECR repositories, and EventBridge schedules:

```bash
cd terraform/pipeline
terraform init
terraform apply
```

Build and push each pipeline image:

```bash
cd pipeline/alpaca  && bash deploy_alpaca_pipeline.sh
cd pipeline/rss     && bash deploy_rss_pipeline.sh
cd pipeline/reddit  && bash deploy_reddit_pipeline.sh
```

### 6. Deploy the RAG service

Provision RAG Lambdas, ChromaDB ECS task, EFS, and API Gateway:

```bash
cd terraform/rag
terraform init
terraform apply
```

Build and push the RAG images:

```bash
cd rag_service
bash deploy_ingest.sh
bash deploy_query.sh
```

### 7. Deploy the dashboard

Provision the ECS cluster, task definition, and service:

```bash
cd terraform/dashboard
terraform init
terraform apply
```

Build and push the dashboard image:

```bash
cd dashboard
bash deploy_dashboard.sh
```

The dashboard will be accessible on port 8501 via the ECS public IP.

## How to Run Locally

### Pipelines

Each pipeline can be run directly with Python (requires AWS credentials for Secrets Manager and network access to RDS):

```bash
cd pipeline/alpaca  && python run_pipeline.py
cd pipeline/rss     && python rss_pipeline.py
cd pipeline/reddit  && python pipeline.py
```

### Historical backfill

Seed the RSS table with Hacker News articles from 2024 onward:

```bash
cd pipeline/rss/seed_historical && python seed_rss_table.py
```

Seed Reddit posts from January 2024 onward via the Arctic Shift API:

```bash
cd pipeline/reddit && python historical_pipeline.py
```

### Dashboard

```bash
cd dashboard
pip install -r requirements.txt
streamlit run dashboard.py --server.port 8501
```

### Tests

```bash
# Alpaca
cd pipeline/alpaca && pytest test_alpaca_extract.py test_alpaca_transform_cleaning.py test_alpaca_load.py -v

# RSS
cd pipeline/rss && python -m pytest test/test_rss_extract.py test/test_rss_transform.py -v

# Reddit
cd pipeline/reddit/test_files && python -m pytest -v

# RAG
cd rag_service && pytest tests/ -v
```

## Notes

- **Schedule** — All three pipelines run every 20 minutes, Monday–Friday, 12:00–22:00 UTC via EventBridge Scheduler, covering US market hours plus pre/post-market.
- **Tracked stocks** — 40 tech tickers across mega-cap (AAPL, MSFT, NVDA, AMZN, GOOGL, META, TSLA), platforms (NFLX, ADBE, CRM, ORCL), semiconductors (AVGO, AMD, INTC, QCOM), cloud/SaaS (SNOW, PLTR, SHOP), cybersecurity (PANW, CRWD, FTNT), consumer tech (UBER, ABNB, SPOT, PYPL), fintech (SQ, COIN, HOOD), and more. Full list in `rds_schema/schema.sql` and `pipeline/alpaca/top_100_tech_companies.py`.
- **Sentiment scoring** — RSS and Reddit pipelines both use OpenAI GPT-4o-mini with the same rubric: relevance 0–10, sentiment -1.0 to +1.0, confidence High/Medium/Low. RSS keeps articles with relevance ≥ 6; Reddit keeps posts with relevance ≥ 7.
- **RAG service** — ChromaDB runs as an ECS Fargate task with EFS-backed persistence. Two Lambda functions handle ingestion and querying, exposed via API Gateway.
- **Lambda timeout** — Pipeline Lambdas have a 600-second (10-minute) timeout. RAG ingest is 300 seconds; RAG query is 60 seconds.
- **Region** — All AWS resources are deployed to `eu-west-2` (London).
