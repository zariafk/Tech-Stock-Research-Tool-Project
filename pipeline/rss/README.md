# RSS Pipeline

ETL pipeline that extracts tech news from RSS feeds, enriches with OpenAI relevance/sentiment scoring, and loads to PostgreSQL.

## Quick Start

### Setup
```bash
pip install -r requirements.txt
export OPENAI_API_KEY=your_key
```

### First Run
Initialize the database:
```bash
python3 seed_rss_table.py
```

### Run Pipeline
```bash
python3 rss_pipeline.py
```

Or trigger via AWS Lambda (see `lambda_handler()` in [rss_pipeline.py](rss_pipeline.py)).

## Pipeline Steps

| Step | File | Purpose |
|------|------|---------|
| Extract | [rss_extract_live.py](rss_extract_live.py) | Pulls from TechCrunch & HackerNews RSS feeds; deduplicates against RDS |
| Analyze | [rss_analysis.py](rss_analysis.py) | Scores relevance (0-10) & sentiment (-1.0 to 1.0) using OpenAI GPT-4o-mini |
| Transform | [rss_transform.py](rss_transform.py) | Standardizes schema; validates required columns |
| Load | [rss_load.py](rss_load.py) | Inserts to `rss_article` & `story_stock` tables; ON CONFLICT handles duplicates |

## Files

- **[rss_pipeline.py](rss_pipeline.py)** – Main orchestrator; entry point for local/Lambda execution
- **[rss_extract_live.py](rss_extract_live.py)** – RSS feed extraction with RDS deduplication check
- **[rss_analysis.py](rss_analysis.py)** – OpenAI enrichment for relevance & sentiment
- **[rss_transform.py](rss_transform.py)** – Data cleaning & schema validation
- **[rss_load.py](rss_load.py)** – PostgreSQL insertion with conflict handling
- **[fallback_stock.py](fallback_stock.py)** – Tech universe ticker list
- **[seed_rss_table.py](seed_rss_table.py)** – Database initialization script

## Testing

```bash
python3 -m pytest test/test_rss_extract.py
python3 -m pytest test/test_rss_transform.py
```

## Environment Variables

- `OPENAI_API_KEY` – OpenAI API key
- `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` – RDS credentials (local dev only; Lambda uses Secrets Manager)