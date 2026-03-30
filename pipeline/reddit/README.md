# Reddit ETL Pipeline

Extracts posts from finance/tech subreddits, identifies stock mentions using OpenAI, scores relevance and sentiment, and loads results into PostgreSQL.

## Pipeline Flow

```
Extract → Deduplicate → Transform → Analyse (OpenAI) → Load (RDS)
```

| Step | Description |
|---|---|
| **Extract** | Fetches posts from 12 subreddits via Reddit's JSON API |
| **Deduplicate** | Removes posts already in the database |
| **Transform** | Cleans data, validates types, builds `reddit_post` and `subreddit` tables |
| **Analyse** | Keyword pre-filter, then OpenAI (`gpt-4o-mini`) scores relevance (0–10) and sentiment (-1.0 to 1.0). Only 7+ relevance kept. Multithreaded |
| **Load** | Inserts into PostgreSQL with upsert logic for subreddits |

A historical backfill pipeline (`historical_pipeline.py`) fetches posts back to January 2024 via the [Arctic Shift API](https://arctic-shift.photon-reddit.com/) and shares the same transform, analysis, and load steps.

## Setup

### Secrets Manager

The pipeline reads from a single AWS secret (`c22-trade-research-tool-secrets`):

```bash
aws secretsmanager create-secret \
  --name "c22-trade-research-tool-secrets" \
  --secret-string '{"host":"...","port":5432,"dbname":"...","username":"...","password":"...","OPENAI_API_KEY":"sk-..."}' \
  --region eu-west-2
```

### Database

The `stock` table must be pre-populated with tickers. The pipeline writes to:

| Table | Key Columns |
|---|---|
| `subreddit` | `subreddit_id`, `subreddit_name`, `subreddit_subscribers` |
| `reddit_post` | `post_id`, `title`, `contents`, `flair`, `score`, `ups`, `upvote_ratio`, `num_comments`, `author`, `created_at`, `permalink`, `url`, `subreddit_id` |
| `story_stock` | `story_id`, `stock_id`, `sentiment_score`, `relevance_score`, `analysis`, `story_type` |

## Usage

**Locally:**

```bash
pip install -r requirements.txt
python pipeline.py                # live
python historical_pipeline.py     # backfill
```

**Deploy to ECR:**

Update `AWS_ACCOUNT_ID` and `REPO_NAME` in `deploy.sh`, then:

```bash
chmod +x deploy.sh && ./deploy.sh
```

Builds a `linux/amd64` Lambda image and pushes to `<account-id>.dkr.ecr.eu-west-2.amazonaws.com/<repo-name>:latest`.

## Project Structure

```
pipeline.py              # Orchestrator (live)
historical_pipeline.py   # Orchestrator (backfill)
extract.py               # Reddit JSON API client
historical_extract.py    # Arctic Shift API client
deduplicate.py           # Raw post deduplication
transform.py             # Cleaning, validation, table building
analysis.py              # OpenAI ticker relevance & sentiment
load.py                  # PostgreSQL insertion & helpers
Dockerfile               # Lambda container image
deploy.sh                # ECR build & push script
requirements.txt         # Python dependencies
```