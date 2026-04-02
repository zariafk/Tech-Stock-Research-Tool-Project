# RSS Pipeline

## Overview

ETL pipeline that extracts tech news articles from RSS feeds, scores them for relevance and sentiment using OpenAI, and loads the results into PostgreSQL.

The pipeline runs in four stages:

1. **Extract** — `rss_extract_live.py` pulls articles from TechCrunch, Hacker News, and The Verge RSS feeds, deduplicating against articles already stored in RDS.
2. **Analyse** — `rss_analysis.py` sends each article to OpenAI GPT-4o-mini, which returns a relevance score (0–10), sentiment (-1.0 to 1.0), confidence, matched ticker, and a short analysis.
3. **Transform** — `rss_transform.py` standardises the schema, validates required columns, and forwards articles to the RAG ingestion endpoint.
4. **Load** — `rss_load.py` inserts rows into `rss_article` and `rss_analysis` tables with `ON CONFLICT` handling on the article URL.

`rss_pipeline.py` orchestrates the full flow and exposes a `lambda_handler` for AWS Lambda invocation.

### Directory structure

```
rss/
├── rss_pipeline.py              # Orchestrator (Lambda + local entry point)
├── rss_extract_live.py          # RSS feed extraction with RDS dedup
├── rss_analysis.py              # OpenAI relevance/sentiment scoring
├── rss_transform.py             # Schema validation and RAG ingest call
├── rss_load.py                  # PostgreSQL insertion
├── rag_ingest_invoke.py         # Triggers RAG ingestion Lambda
├── fallback_stock.py            # Ticker-to-company-name mapping
├── logger.py                    # Shared logging config
├── deploy_rss_pipeline.sh       # Build and push Docker image to ECR
├── dockerfile                   # Lambda Python 3.12 container image
├── requirements.txt             # Python dependencies
├── seed_historical/
│   ├── seed_rss_table.py        # Backfill script (Hacker News via Algolia)
│   └── rss_extract_historical.py
└── test/
    ├── test_rss_extract.py
    └── test_rss_transform.py
```

## What You Need

### Credentials (via AWS Secrets Manager)

All credentials are stored in the secret `c22-trade-research-tool-secrets` and loaded automatically when running on Lambda. For local development, set these environment variables:

| Variable | Purpose |
|----------|---------|
| `OPENAI_API_KEY` | OpenAI API key for GPT-4o-mini analysis |
| `DB_HOST` | RDS PostgreSQL host |
| `DB_NAME` | Database name |
| `DB_USER` | Database user |
| `DB_PASSWORD` | Database password |

### Python dependencies

```
feedparser  pytest  requests  openai  python-dotenv
boto3  pyarrow  pandas  psycopg2-binary
```

Install locally:

```bash
pip install -r requirements.txt
```

## How to Deploy

The pipeline runs as an AWS Lambda function behind a Docker container image stored in ECR.

### 1. Build and push the image

```bash
bash deploy_rss_pipeline.sh
```

This script authenticates with ECR, builds a `linux/amd64` image from the `dockerfile`, tags it, and pushes to the `c22-stocksiphon-rss-ecr` repository in `eu-west-2`.

### 2. Provision infrastructure with Terraform

```bash
cd ../../terraform/pipeline
terraform init
terraform apply
```

Terraform creates the Lambda function, ECR repository, and an EventBridge Scheduler rule that triggers the pipeline every 20 minutes, Monday–Friday, 12:00–22:00 UTC.

## How to Run

### Locally

```bash
python3 rss_pipeline.py
```

### Via Lambda (CLI)

```bash
aws lambda invoke \
  --function-name c22-stocksiphon-rss-pipeline \
  --region eu-west-2 \
  output.json
```

### Seed historical data

To backfill the database with Hacker News articles from 2024 onward:

```bash
cd seed_historical
python3 seed_rss_table.py
```

### Run tests

```bash
python3 -m pytest test/test_rss_extract.py
python3 -m pytest test/test_rss_transform.py
```

## Notes

- **RSS feeds**: TechCrunch, Hacker News, The Verge. Defined in `RSS_FEEDS` inside `rss_extract_live.py`.
- **Deduplication**: The extract step queries RDS for the latest stored article date per feed and only pulls newer entries. The load step uses `ON CONFLICT (url)` as a secondary guard.
- **OpenAI costs**: Analysis uses `gpt-4o-mini` with a `ThreadPoolExecutor` for parallel scoring. Each article produces one API call.
- **RAG ingestion**: After transform, articles are forwarded to the RAG ingestion Lambda so they appear in the company-search chatbot.
- **Fallback ticker mapping**: `fallback_stock.py` provides a hardcoded ticker-to-company-name dictionary, used when the database lookup is unavailable.
- **Historical backfill**: `seed_historical/seed_rss_table.py` reads tickers from the `stock` table, queries the Hacker News Algolia API, runs the same analysis/transform/load chain, and writes to RDS.