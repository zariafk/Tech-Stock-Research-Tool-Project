# Reddit Pipeline

## Overview

ETL pipeline that extracts posts from finance and tech subreddits via Reddit's JSON API, identifies stock mentions and scores relevance/sentiment using OpenAI, and loads results into PostgreSQL.

The pipeline runs in five stages:

1. **Extract** — `extract.py` fetches recent posts from 12 subreddits using Reddit's public JSON endpoints.
2. **Deduplicate** — `deduplicate.py` removes posts that already exist in the database.
3. **Transform** — `transform.py` cleans the data, validates types, and builds `reddit_post` and `subreddit` DataFrames.
4. **Analyse** — `analysis.py` applies a keyword pre-filter, then sends posts to OpenAI GPT-4o-mini for relevance (0–10) and sentiment (-1.0 to 1.0) scoring. Only posts with relevance ≥ 7 are kept. Runs multithreaded.
5. **Load** — `load.py` inserts into `subreddit`, `reddit_post`, and `story_stock` tables with upsert logic.

`pipeline.py` orchestrates the full flow and exposes a `lambda_handler` for AWS Lambda invocation.

A separate **historical backfill** pipeline (`historical_pipeline.py`) fetches posts back to January 2024 via the [Arctic Shift API](https://arctic-shift.photon-reddit.com/) and shares the same transform and load steps.

### Directory structure

```
reddit/
├── pipeline.py                # Orchestrator — live (Lambda + local entry point)
├── historical_pipeline.py     # Orchestrator — historical backfill
├── extract.py                 # Reddit JSON API client
├── historical_extract.py      # Arctic Shift API client
├── deduplicate.py             # Raw post deduplication
├── transform.py               # Cleaning, validation, table building
├── analysis.py                # OpenAI ticker relevance & sentiment
├── load.py                    # PostgreSQL insertion & helpers
├── rag_ingest_invoke.py       # Triggers RAG ingestion Lambda
├── deploy_reddit_pipeline.sh  # Build and push Docker image to ECR
├── Dockerfile                 # Lambda Python 3.12 container image
├── requirements.txt           # Python dependencies
└── test_files/
    ├── conftest.py
    ├── test_extract.py
    ├── test_deduplicate.py
    ├── test_transform.py
    └── test_load.py
```

### Database tables

| Table | Key columns |
|---|---|
| `subreddit` | `subreddit_id`, `subreddit_name`, `subreddit_subscribers` |
| `reddit_post` | `post_id`, `title`, `contents`, `flair`, `score`, `ups`, `upvote_ratio`, `num_comments`, `author`, `created_at`, `permalink`, `url`, `subreddit_id` |
| `story_stock` | `story_id`, `stock_id`, `sentiment_score`, `relevance_score`, `analysis`, `story_type` |

## What You Need

### Credentials (via AWS Secrets Manager)

All credentials are stored in the secret `c22-trade-research-tool-secrets` and loaded automatically when running on Lambda. For local development, ensure your AWS credentials can access the secret.

| Key | Purpose |
|---|---|
| `OPENAI_API_KEY` | OpenAI API key for GPT-4o-mini analysis |
| `host` | RDS PostgreSQL host |
| `port` | RDS port (default `5432`) |
| `dbname` | Database name |
| `username` | Database user |
| `password` | Database password |

### Python dependencies

```
requests  pandas  boto3  pyarrow  psycopg2-binary  openai
```

Install locally:

```bash
pip install -r requirements.txt
```

### Prerequisites

- RDS database with schema initialised (`rds_schema/schema.sql`) and the `stock` table pre-populated with tickers.
- AWS credentials configured locally (`aws configure`) or via IAM role.

## How to Deploy

The pipeline runs as an AWS Lambda function behind a Docker container image stored in ECR.

### 1. Build and push the image

```bash
bash deploy_reddit_pipeline.sh
```

This script authenticates with ECR, builds a `linux/amd64` image from the `Dockerfile`, tags it, and pushes to the `c22-stocksiphon-reddit-ecr` repository in `eu-west-2`.

### 2. Provision infrastructure with Terraform

```bash
cd ../../terraform/pipeline
terraform init
terraform apply
```

Terraform creates the Lambda function, ECR repository, and an EventBridge Scheduler rule that triggers the pipeline every 20 minutes, Monday–Friday, 12:00–22:00 UTC.

## How to Run

### Locally (live)

```bash
python pipeline.py
```

### Locally (historical backfill)

```bash
python historical_pipeline.py
```

Fetches posts from January 2024 onward via the Arctic Shift API.

### Via Lambda (CLI)

```bash
aws lambda invoke \
  --function-name c22-stocksiphon-reddit-pipeline \
  --region eu-west-2 \
  output.json
```

### Run tests

```bash
cd test_files
python -m pytest test_extract.py test_deduplicate.py test_transform.py test_load.py -v
```

## Notes

- **Subreddits** — 12 tracked: trading, stocks, investing, stockmarket, valueinvesting, options, algotrading, semiconductors, artificialinteligence, cloudcomputing, hardware, wallstreetbets. Defined in `SUBREDDITS` inside `pipeline.py`.
- **Relevance threshold** — only posts scoring 7 or above on the 0–10 relevance scale are kept after analysis.
- **OpenAI costs** — analysis uses `gpt-4o-mini` with multithreaded calls. Each post produces one API call.
- **RAG ingestion** — after analysis, matched posts are forwarded to the RAG ingestion Lambda so they appear in the company-search chatbot.
- **Deduplication** — the extract step queries RDS for existing post IDs and skips them before processing. The load step uses upsert logic on the `subreddit` table as a secondary guard.
- **Historical backfill** — `historical_pipeline.py` uses the Arctic Shift Algolia API to pull Hacker News–style historical data from January 2024 onward, then runs the same transform and load steps.