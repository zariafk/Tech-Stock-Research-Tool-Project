"""Seed script: backfill rss_article + story_stock from Hacker News historical data.

Flow: stock table → extract_historical → analysis → transform → load (RDS)
"""
import sys
from pathlib import Path

# Add parent directory to path FIRST (before imports)
sys.path.insert(0, str(Path(__file__).parent.parent))

from logger import logger
from rss_load import load, get_connection, get_tickers_from_db
from rss_transform import transform
from rss_analysis import analysis
from rss_extract_historical import extract_historical
import dotenv


def get_tickers_map() -> dict:
    """Read ticker → company name from the stock table in RDS."""
    conn = get_connection()
    try:
        stock_lookup = get_tickers_from_db(conn)
    finally:
        conn.close()

    # { ticker: company_name } — matches format expected by extract_historical
    return {
        ticker: info["stock_name"]
        for ticker, info in stock_lookup.items()
    }


def seed():
    """Run full historical backfill pipeline."""
    # Step 1: Read tickers from RDS stock table
    tickers_map = get_tickers_map()
    tickers = list(tickers_map.keys())
    logger.info("Loaded %d tickers from stock table.", len(tickers))

    # Step 2: Extract historical articles from Hacker News
    raw_articles = extract_historical(tickers_map)
    logger.info("Extracted %d raw historical articles.", len(raw_articles))

    if not raw_articles:
        logger.warning("No historical articles found. Exiting.")
        return

    # Step 3: OpenAI analysis — sentiment, relevance, filtering
    analysed_df = analysis(raw_articles, tickers)
    logger.info("Analysis complete. %d articles after filtering.",
                len(analysed_df))

    if analysed_df.empty:
        logger.warning("No articles survived analysis. Exiting.")
        return

    # Step 4: Transform — clean, normalise dates, deduplicate
    transformed_df = transform(analysed_df)
    logger.info("Transform complete. %d articles ready to load.",
                len(transformed_df))

    # Step 5: Load into rss_article + story_stock tables
    net_new = load(transformed_df)
    logger.info("Seed complete. %d net new articles inserted.", net_new)


if __name__ == "__main__":
    seed()
