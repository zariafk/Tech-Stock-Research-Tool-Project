"""Main pipeline orchestrator."""

import logging
from datetime import datetime, timezone

from historical_extract import extract_historical
from deduplicate import deduplicate_raw_posts
from transform import transform_main
from load import get_secret, get_connection, get_existing_ids, load_main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RDS_SECRET_NAME = "c22-trade-research-tool-secrets"

SUBREDDITS = [
    "trading", "stocks", "investing", "stockmarket",
    "valueinvesting", "options", "algotrading", "semiconductors",
    "artificialinteligence", "cloudcomputing", "hardware",
    "wallstreetbets",
]

FACT_POSTS_COLUMNS = [
    "id", "title", "selftext", "link_flair_text", "score",
    "ups", "upvote_ratio", "num_comments", "author",
    "created_utc", "permalink", "url", "subreddit_id",
]

DIM_SUBREDDITS_COLUMNS = [
    "subreddit_id", "subreddit", "subreddit_subscribers",
]

REQUIRED_COLUMNS = ["id", "title", "subreddit_id", "author"]


def run_pipeline() -> None:
    """Runs the full ETL pipeline."""
    secret = get_secret(RDS_SECRET_NAME)
    conn = get_connection(secret)

    try:
        logger.info("Starting historical extract")
        raw_posts = extract_historical(
            SUBREDDITS,
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2026, 3, 25, tzinfo=timezone.utc),
        )

        logger.info("Deduplicating raw posts")
        existing_post_ids = get_existing_ids(conn, "fact_posts", "id")
        raw_posts = deduplicate_raw_posts(raw_posts, existing_post_ids)

        logger.info("Starting transform")
        fact_posts, dim_subreddits = transform_main(
            raw_posts,
            fact_columns=FACT_POSTS_COLUMNS,
            dim_columns=DIM_SUBREDDITS_COLUMNS,
            required_columns=REQUIRED_COLUMNS,
        )

        logger.info("Starting load")
        load_main(
            {
                "fact_posts": fact_posts,
                "dim_subreddits": dim_subreddits,
            },
            conn=conn,
        )

        logger.info("Pipeline complete")

    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
