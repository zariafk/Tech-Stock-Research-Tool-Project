"""Main pipeline orchestrator."""

from top_100_tech_companies import tech_universe
import logging
import os

import dotenv
from openai import OpenAI

from extract import extract_main
from deduplicate import deduplicate_raw_posts
from transform import transform_main
from analysis import analyse_posts
from load import get_secret, get_connection, get_existing_ids, load_main

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RDS_SECRET_NAME = "reddit-pipeline/rds-credentials"

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

# Import your existing ticker universe
TICKER_COMPANIES = tech_universe


def run_pipeline() -> None:
    """Runs the full ETL pipeline."""
    secret = get_secret(RDS_SECRET_NAME)
    conn = get_connection(secret)
    openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    try:
        logger.info("Starting extract")
        raw_posts = extract_main(SUBREDDITS, include_comments=False)

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

        logger.info("Starting analysis")
        fact_post_tickers = analyse_posts(
            fact_posts,
            ticker_companies=TICKER_COMPANIES,
            client=openai_client,
        )

        logger.info("Starting load")
        print(fact_post_tickers)
        # load_main(
        #     {
        #         "fact_posts": fact_posts,
        #         "dim_subreddits": dim_subreddits,
        #         "fact_post_tickers": fact_post_tickers,
        #     },
        #     conn=conn,
        # )

        logger.info("Pipeline complete")

    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
