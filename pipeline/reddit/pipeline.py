"""Main pipeline orchestrator."""

import logging
import os
import json

from openai import OpenAI

from extract import extract_main
from deduplicate import deduplicate_raw_posts
from transform import transform_main
from analysis import analyse_posts
from load import get_secret, get_connection, get_existing_ids, load_main, join_tables_to_json, get_stock_id_map, build_story_stock_df

from rag_ingest_invoke import invoke_rag_ingest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SECRET_NAME = "c22-trade-research-tool-secrets"

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

FACT_POSTS_RENAME = {
    "id": "post_id",
    "selftext": "contents",
    "link_flair_text": "flair",
    "created_utc": "created_at",
}

DIM_SUBREDDITS_COLUMNS = [
    "subreddit_id", "subreddit", "subreddit_subscribers",
]

REQUIRED_COLUMNS = ["id", "title", "subreddit_id", "author"]


def get_ticker_companies(conn) -> dict[str, str]:
    """Fetches ticker -> stock_name mapping from the stock table."""
    query = "SELECT ticker, stock_name FROM stock"
    with conn.cursor() as cur:
        cur.execute(query)
        return {ticker: name for ticker, name in cur.fetchall()}


def run_pipeline() -> None:
    """Runs the full ETL pipeline."""
    secret = get_secret(SECRET_NAME)
    conn = get_connection(secret)
    openai_client = OpenAI(api_key=secret["OPENAI_API_KEY"])

    try:
        logger.info("Starting extract")
        raw_posts = extract_main(SUBREDDITS, include_comments=False)

        logger.info("Deduplicating raw posts")
        existing_post_ids = get_existing_ids(conn, "reddit_post", "post_id")
        raw_posts = deduplicate_raw_posts(raw_posts, existing_post_ids)

        logger.info("Starting transform")
        fact_posts, dim_subreddits = transform_main(
            raw_posts,
            fact_columns=FACT_POSTS_COLUMNS,
            dim_columns=DIM_SUBREDDITS_COLUMNS,
            required_columns=REQUIRED_COLUMNS,
        )

        fact_posts = fact_posts.rename(columns=FACT_POSTS_RENAME)

        ticker_companies = get_ticker_companies(conn)
        logger.info("Loaded %d tickers from stock table",
                    len(ticker_companies))

        logger.info("Starting analysis")
        fact_post_tickers = analyse_posts(
            fact_posts,
            ticker_companies=ticker_companies,
            client=openai_client,
        )

        # Only keep posts that have at least one relevant ticker
        matched_post_ids = set(fact_post_tickers["post_id"])
        fact_posts = fact_posts[fact_posts["post_id"].isin(matched_post_ids)]
        logger.info("Kept %d posts with relevant tickers", len(fact_posts))

        logger.info("Starting load")

        result = join_tables_to_json(
            fact_posts, dim_subreddits, fact_post_tickers)

        if result:
            logger.info("Invoking RAG ingest with %d posts",
                        len(result["rag_dict"]))
            invoke_rag_ingest(source="reddit", data=result["rag_dict"])
        logger.info("RAG ingest invoked successfully")

        logger.info("Starting load")

        stock_id_map = get_stock_id_map(conn)
        story_stock = build_story_stock_df(fact_post_tickers, stock_id_map)

        load_main(
            {
                "subreddit": dim_subreddits,
                "reddit_post": fact_posts,
                "reddit_analysis": story_stock,
            },
            conn=conn,
            conflict_columns={
                "subreddit": "subreddit_id",
            },
        )
        logger.info("Pipeline complete")

    finally:
        conn.close()


def lambda_handler(event, context):
    """Allows the lambda function to run the pipeline."""
    run_pipeline()


if __name__ == "__main__":
    run_pipeline()
