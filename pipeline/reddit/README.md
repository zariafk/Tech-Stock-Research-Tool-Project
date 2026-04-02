# Reddit ETL Pipeline

## Overview

Scrapes finance and tech subreddits, uses OpenAI to score stock relevance and sentiment, and loads structured results into PostgreSQL — deployed as an AWS Lambda container.

```
Extract  →  Deduplicate  →  Transform  →  Analyse  →  Load
```

**Extract** pulls posts from 12 subreddits via the [Arctic Shift API](https://arctic-shift.photon-reddit.com/). **Deduplicate** drops posts already in the database. **Transform** cleans, validates, and shapes the data into `reddit_post` and `subreddit` tables. **Analyse** keyword-filters posts then sends matches to GPT-4o-mini for relevance (0–10) and sentiment (-1.0 to 1.0) scoring — only posts scoring 7+ are kept. **Load** upserts results into PostgreSQL and invokes a RAG ingest Lambda for downstream search.

A historical backfill pipeline (`historical_pipeline.py`) covers posts back to January 2024 and shares the same transform, analysis, and load steps.

## What You Need

**Python dependencies** — install via `pip install -r requirements.txt`. Key packages: `requests`, `pandas`, `boto3`, `psycopg2-binary`, `openai`.

**AWS Secrets Manager** — a single secret (`c22-trade-research-tool-secrets`) containing:

```json
{
  "host": "...",
  "port": 5432,
  "dbname": "...",
  "username": "...",
  "password": "...",
  "OPENAI_API_KEY": "sk-..."
}
```

**Database** — the `stock` table must be pre-populated with tickers and company names. The pipeline writes to:

| Table | Purpose |
|---|---|
| `subreddit` | Subreddit metadata (name, subscriber count) |
| `reddit_post` | Post content, scores, timestamps, and flair |
| `reddit_analysis` | Per-post ticker sentiment, relevance, confidence, and analysis |

## How to Deploy

The deploy script builds a `linux/amd64` Lambda image and pushes it to ECR (`c22-stocksiphon-reddit-ecr`):

```bash
chmod +x deploy_reddit_pipeline.sh
./deploy_reddit_pipeline.sh
```

## How to Run

```bash
python pipeline.py              # live run
python historical_pipeline.py   # backfill
pytest                          # run tests
```

Unit tests cover each pipeline stage in isolation — API calls and database connections are fully mocked. The suite validates extraction and retry logic, deduplication filtering, transform operations (timestamp conversion, numeric clamping, null/deleted row removal), and database insertion with conflict handling.

## Notes

- The live pipeline is triggered via `pipeline.lambda_handler` when deployed as a Lambda.
- The historical backfill extracts day-by-day using Arctic Shift's `after`/`before` timestamp parameters and paginates automatically.
- Analysis runs multithreaded (up to 20 workers) to parallelise OpenAI calls. Posts are keyword-pre-filtered before being sent to the API to minimise cost.
- The RAG ingest step (`rag_ingest_invoke.py`) calls a separate Lambda (`c22-stocksiphon-rag-ingest-lambda`) synchronously and raises on failure.

### Project Structure

```
pipeline.py                  Live pipeline orchestrator
historical_pipeline.py       Historical backfill orchestrator
extract.py                   Arctic Shift API client
historical_extract.py        Date-range historical extraction
deduplicate.py               Filters already-seen posts
transform.py                 Cleaning, validation, table building
analysis.py                  OpenAI ticker scoring (multithreaded)
load.py                      PostgreSQL insertion & helpers
rag_ingest_invoke.py         Lambda invocation for RAG pipeline
Dockerfile                   Lambda container image
deploy_reddit_pipeline.sh    ECR build & push script
requirements.txt             Python dependencies
test_files/
    conftest.py              Shared fixtures (sample posts, column lists)
    test_extract.py          API client, retry, and comment fetching
    test_deduplicate.py      Duplicate filtering logic
    test_transform.py        Flattening, validation, timestamp conversion
    test_load.py             Database insertion and upsert handling
```