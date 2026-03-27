# Reddit ETL Pipeline

An automated pipeline that extracts posts from finance and tech subreddits, identifies mentions of tracked stocks using OpenAI, scores them for relevance and sentiment, and loads the results into a PostgreSQL RDS instance.

## Pipeline Flow

```
Extract → Deduplicate → Transform → Analyse (OpenAI) → Load (RDS)
```

**Extract** — Fetches recent posts from 12 subreddits via Reddit's public JSON API.

**Deduplicate** — Filters out posts already stored in the database by checking existing post IDs.

**Transform** — Flattens raw JSON into two tables (`reddit_post`, `subreddit`), validates data types, cleans nulls and deleted content, and converts timestamps.

**Analyse** — Pre-filters posts for ticker/company keyword matches, then sends matched posts to OpenAI (`gpt-4o-mini`) for relevance scoring (0–10) and sentiment analysis (-1.0 to 1.0). Only posts scoring 7+ relevance are kept. Uses multithreaded API calls for throughput.

**Load** — Inserts `subreddit`, `reddit_post`, and `story_stock` tables into PostgreSQL. Subreddits use upsert logic to handle duplicates; posts and analysis rows are deduplicated upstream.

## Configuration

All configuration lives in `pipeline.py`: subreddit list, column definitions, rename mappings, and secret names. Individual scripts receive everything as parameters and have no hardcoded config.

### AWS Secrets Manager

The pipeline reads credentials from a single secret (`c22-trade-research-tool-secrets`) containing:

```json
{
  "host": "your-rds-endpoint.rds.amazonaws.com",
  "port": 5432,
  "dbname": "your_db",
  "username": "your_user",
  "password": "your_password",
  "OPENAI_API_KEY": "sk-..."
}
```

Create it with:

```bash
aws secretsmanager create-secret \
  --name "c22-trade-research-tool-secrets" \
  --secret-string '{"host":"...","port":5432,"dbname":"...","username":"...","password":"...","OPENAI_API_KEY":"sk-..."}' \
  --region eu-west-2
```

## Database Schema

The pipeline writes to three tables:

| Table | Key Columns |
|---|---|
| `subreddit` | `subreddit_id`, `subreddit_name`, `subreddit_subscribers` |
| `reddit_post` | `post_id`, `title`, `contents`, `flair`, `score`, `ups`, `upvote_ratio`, `num_comments`, `author`, `created_at`, `permalink`, `url`, `subreddit_id` |
| `story_stock` | `story_id`, `stock_id`, `sentiment_score`, `relevance_score`, `analysis`, `story_type` |

`story_stock` links posts to entries in the `stock` table (which must be pre-populated with tickers).

## Historical Backfill

A separate pipeline (`historical_pipeline.py`) uses the [Arctic Shift API](https://arctic-shift.photon-reddit.com/) to fetch posts dating back to January 2024. It shares the same transform, analysis, and load steps.

```bash
python historical_pipeline.py
```

## Dockerise and Deploy to ECR

### Build and push

The `deploy.sh` script handles authentication, building, and pushing to ECR. Update the placeholders first:

```bash
# In deploy.sh, replace:
AWS_ACCOUNT_ID=<aws-account-id>
REPO_NAME=<repo-name>
```

Then run:

```bash
chmod +x deploy.sh
./deploy.sh
```

This builds a `linux/amd64` image and pushes it to your ECR repository at `<account-id>.dkr.ecr.eu-west-2.amazonaws.com/<repo-name>:latest`.

### What the Docker image contains

The `Dockerfile` packages the pipeline as an AWS Lambda function using the `python:3.12` Lambda base image. It installs dependencies from `requirements.txt` and sets the handler to `pipeline.lambda_handler`.

## Running Locally

```bash
pip install -r requirements.txt
python pipeline.py
```

## Project Structure

```
├── pipeline.py              # Orchestrator (live)
├── historical_pipeline.py   # Orchestrator (backfill)
├── extract.py               # Reddit JSON API client
├── historical_extract.py    # Arctic Shift API client
├── deduplicate.py           # Raw post deduplication
├── transform.py             # Cleaning, validation, table building
├── analysis.py              # OpenAI ticker relevance & sentiment
├── load.py                  # PostgreSQL insertion & helpers
├── Dockerfile               # Lambda container image
├── deploy.sh                # ECR build & push script
└── requirements.txt         # Python dependencies
```