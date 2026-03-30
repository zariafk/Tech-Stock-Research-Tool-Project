"""Load transformed RSS articles to PostgreSQL RDS.

Inserts into:
  - rss_article: title, url, summary, published_date, source
  - rss_analysis: story_id, stock_id, sentiment_score, relevance_score, analysis

Deduplication: ON CONFLICT on url (unique article = unique url).
"""

import json
import os
import psycopg2
import psycopg2.extras
import pandas as pd
import boto3
from logger import logger


def get_secret(secret_name: str, region: str = "eu-west-2") -> dict:
    """Retrieve DB credentials from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_connection():
    """Connect to RDS PostgreSQL. Uses Secrets Manager for prod."""
    secret = get_secret("c22-trade-research-tool-secrets")
    return psycopg2.connect(
        host=secret["host"],
        port=secret.get("port", 5432),
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],

    )


def get_tickers_from_db(conn) -> dict:
    """Read ticker → stock_name mapping from the stock table."""
    with conn.cursor() as cur:
        cur.execute("SELECT stock_id, ticker, stock_name FROM stock")
        rows = cur.fetchall()
    # { ticker: { stock_id, stock_name } }
    return {
        row[1]: {"stock_id": row[0], "stock_name": row[2]}
        for row in rows
    }


def load(df: pd.DataFrame) -> int:
    """Insert articles into rss_article and rss_analysis tables.

    Returns the number of net new articles inserted.
    """
    if df.empty:
        logger.warning("No articles to load.")
        return 0

    conn = get_connection()
    total_net_new = 0

    try:
        # Build ticker → stock_id lookup
        stock_lookup = get_tickers_from_db(conn)

        with conn.cursor() as cur:
            for _, row in df.iterrows():
                ticker = row["ticker"]

                # Skip if ticker not in stock table
                if ticker not in stock_lookup:
                    logger.warning(
                        "Ticker %s not found in stock table — skipping.", ticker)
                    continue

                stock_id = stock_lookup[ticker]["stock_id"]

                # Insert into rss_article; skip duplicates via unique url
                cur.execute(
                    """
                    INSERT INTO rss_article (title, url, summary, published_date, source)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                    RETURNING story_id
                    """,
                    (
                        row["title"],
                        row["url"],
                        row["summary"],
                        row["published_date"],
                        row["source"]
                    )
                )

                result = cur.fetchone()
                if result is None:
                    # Duplicate — already exists
                    continue

                story_id = result[0]

                # Insert into rss_analysis (with fallback if confidence column missing)
                try:
                    cur.execute(
                        """
                        INSERT INTO rss_analysis
                            (story_id, stock_id, sentiment_score, relevance_score, confidence, analysis)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            story_id,
                            stock_id,
                            row.get("sentiment"),
                            row.get("relevance_score"),
                            row.get("confidence", 'Unknown'),
                            row.get("analysis"),
                        ),
                    )
                except psycopg2.errors.UndefinedColumn:
                    # Fallback for tables without confidence column
                    logger.warning(
                        "confidence column missing from rss_analysis—inserting without it")
                    cur.execute(
                        """
                        INSERT INTO rss_analysis
                            (story_id, stock_id, sentiment_score, relevance_score, analysis)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            story_id,
                            stock_id,
                            row.get("sentiment"),
                            row.get("relevance_score"),
                            row.get("analysis"),
                        ),
                    )
                total_net_new += 1

        conn.commit()
        logger.info("Loaded %d net new articles into RDS.", total_net_new)

    except Exception:
        conn.rollback()
        logger.error("Load failed — transaction rolled back.", exc_info=True)
        raise
    finally:
        conn.close()

    return total_net_new


if __name__ == "__main__":
    import glob
    import dotenv
    dotenv.load_dotenv()

    csv_files = sorted(glob.glob("test_results_*.csv"))
    if not csv_files:
        print("No test CSV found. Run rss_extract.py first.")
    else:
        latest = csv_files[-1]
        df = pd.read_csv(latest)
        print(f"Loaded {len(df)} articles from {latest}")
        net_new = load(df)
        print(f"Net new articles inserted into RDS: {net_new}")
