# RDS Schema

## Overview

PostgreSQL schema for the Tech Stock Research Tool. Defines all tables used by the three ETL pipelines (Alpaca, RSS, Reddit), their analysis outputs, and the core stock lookup table seeded with 40 tracked tech tickers.

### Tables

| Table | Purpose |
|---|---|
| `stock` | Core lookup — ticker and company name for all tracked equities (seeded on creation) |
| `rss_article` | RSS/Hacker News articles with title, URL, summary, published date, source |
| `rss_analysis` | AI-enriched scores per article–stock pair (sentiment, relevance, confidence, analysis text) |
| `subreddit` | Reddit subreddit dimension table (ID, name, subscriber count) |
| `reddit_post` | Reddit posts with title, content, flair, score, comments, author, timestamps |
| `reddit_analysis` | AI-enriched scores per post–stock pair (same schema as `rss_analysis`) |
| `alpaca_live` | Latest intraday OHLCV bars from Alpaca |
| `alpaca_history` | Historical daily OHLCV bars from Alpaca |

See `ERD.png` for a visual diagram of all table relationships.

### Directory structure

```
rds_schema/
├── schema.sql                  # Full DDL + stock seed data
├── create_rds.py               # Python script to execute schema against RDS
├── migrate_add_confidence.sql  # Migration: adds confidence column to reddit_analysis
├── ERD.png                     # Entity-relationship diagram
└── README.md
```

## What You Need

### Credentials

`create_rds.py` reads database credentials from environment variables (or a `.env` file via `python-dotenv`):

| Variable | Purpose |
|---|---|
| `DB_HOST` | RDS PostgreSQL host |
| `DB_PORT` | RDS port (default `5432`) |
| `DB_USER` | Database user |
| `DB_PASSWORD` | Database password |
| `DB_NAME` | Database name |

### Python dependencies

```
psycopg2-binary  python-dotenv
```

Install:

```bash
pip install -r requirements.txt
```

### Prerequisites

- A running PostgreSQL RDS instance (provisioned via `terraform/database/`).
- Network access to the RDS endpoint (the instance is publicly accessible by default).
- SSL is required (`sslmode=require` in the connection).

## How to Deploy

### 1. Provision the RDS instance

If not already created:

```bash
cd ../terraform/database
terraform init
terraform apply
```

This creates a PostgreSQL 16 instance (`db.t3.micro`, 20 GB) in `eu-west-2`.

### 2. Set environment variables

```bash
export DB_HOST="your-rds-endpoint"
export DB_PORT="5432"
export DB_USER="your-user"
export DB_PASSWORD="your-password"
export DB_NAME="your-dbname"
```

Or create a `.env` file in this directory:

```
DB_HOST=your-rds-endpoint
DB_PORT=5432
DB_USER=your-user
DB_PASSWORD=your-password
DB_NAME=your-dbname
```

### 3. Run the schema script

```bash
python create_rds.py
```

This reads `schema.sql`, executes each statement against RDS, and seeds the `stock` table with 40 tech tickers.

## How to Run

### Create or reset the schema

```bash
python create_rds.py
```

The schema uses `DROP TABLE IF EXISTS ... CASCADE` before each `CREATE`, so re-running the script will fully reset all tables and re-seed the stock data.

### Apply migrations

Run migration scripts directly against the database:

```bash
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -f migrate_add_confidence.sql
```

## Notes

- **Destructive reset** — `schema.sql` drops all tables before recreating them. Do not run `create_rds.py` against a production database with data you want to keep.
- **Seed data** — The `stock` table is seeded with 40 tickers via `INSERT ... ON CONFLICT (ticker) DO NOTHING`, so re-running is safe for that table alone.
- **Analysis tables** — Both `rss_analysis` and `reddit_analysis` use composite primary keys (`story_id`, `stock_id`) and a `confidence` column with a CHECK constraint limited to High, Medium, Low, or Unknown.
- **Foreign keys** — `reddit_post` references `subreddit`; both analysis tables reference `stock`; `rss_analysis` references `rss_article`. The `CASCADE` drop order in the schema respects these dependencies.