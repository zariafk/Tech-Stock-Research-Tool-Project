"""Main pipeline orchestrator — processes data in per-subreddit-per-day batches
to avoid loading millions of posts into memory at once."""

from datetime import datetime, timezone
import logging

from openai import OpenAI

from historical_extract import fetch_day, generate_date_range, DEFAULT_MAX_POSTS
from deduplicate import deduplicate_raw_posts
from transform import transform_main
from analysis import analyse_posts
from load import (
    get_secret,
    get_connection,
    get_existing_ids,
    load_main,
    get_stock_id_map,
    build_story_stock_df,
)


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


def process_batch(
    raw_posts: list[dict],
    *,
    conn,
    openai_client: OpenAI,
    ticker_companies: dict[str, str],
    stock_id_map: dict[str, int],
    seen_ids: set[str],
) -> int:
    """Runs transform → analyse → load for a single batch of raw posts.

    Returns the number of posts loaded into the database.
    """
    # Deduplicate against posts already processed this run
    raw_posts = deduplicate_raw_posts(raw_posts, seen_ids)
    if not raw_posts:
        return 0

    # Track these IDs so future batches skip them
    for post in raw_posts:
        post_id = post.get("data", {}).get("id")
        if post_id:
            seen_ids.add(post_id)

    # Transform
    fact_posts, dim_subreddits = transform_main(
        raw_posts,
        fact_columns=FACT_POSTS_COLUMNS,
        dim_columns=DIM_SUBREDDITS_COLUMNS,
        required_columns=REQUIRED_COLUMNS,
    )

    fact_posts = fact_posts.rename(columns=FACT_POSTS_RENAME)

    if fact_posts.empty:
        return 0

    # Analyse
    fact_post_tickers = analyse_posts(
        fact_posts,
        ticker_companies=ticker_companies,
        client=openai_client,
    )

    # Only keep posts that have at least one relevant ticker
    if not fact_post_tickers.empty:
        matched_post_ids = set(fact_post_tickers["post_id"])
        fact_posts = fact_posts[fact_posts["post_id"].isin(matched_post_ids)]
    else:
        # No ticker matches — still load the subreddit dimension rows
        # but skip posts and analysis
        load_main(
            {"subreddit": dim_subreddits},
            conn=conn,
            conflict_columns={"subreddit": "subreddit_id"},
        )
        return 0

    # Build story_stock mapping
    story_stock = build_story_stock_df(fact_post_tickers, stock_id_map)

    # Load into RDS
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

    return len(fact_posts)


def run_pipeline() -> None:
    """Runs the full ETL pipeline in per-subreddit-per-day batches."""
    secret = get_secret(SECRET_NAME)
    conn = get_connection(secret)
    openai_client = OpenAI(api_key=secret["OPENAI_API_KEY"])

    try:
        ticker_companies = get_ticker_companies(conn)
        stock_id_map = get_stock_id_map(conn)
        logger.info("Loaded %d tickers from stock table",
                    len(ticker_companies))

        # Seed seen_ids with anything already in the database so we
        # don't re-insert posts from a previous partial run
        seen_ids = get_existing_ids(conn, "reddit_post", "post_id")

        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2026, 3, 27, tzinfo=timezone.utc)
        dates = generate_date_range(start_date, end_date)

        total_loaded = 0

        for subreddit in SUBREDDITS:
            subreddit_loaded = 0

            for date in dates:
                logger.info(
                    "Processing r/%s — %s",
                    subreddit, date.strftime("%Y-%m-%d"),
                )

                # Extract a single day (capped at top posts by engagement)
                posts = fetch_day(subreddit, date, max_posts=DEFAULT_MAX_POSTS)
                if not posts:
                    continue

                # Wrap in the {"data": ...} structure expected by transform
                raw_posts = [{"data": post} for post in posts]

                # Run the full ETL for this batch
                loaded = process_batch(
                    raw_posts,
                    conn=conn,
                    openai_client=openai_client,
                    ticker_companies=ticker_companies,
                    stock_id_map=stock_id_map,
                    seen_ids=seen_ids,
                )

                subreddit_loaded += loaded

            logger.info(
                "Completed r/%s — %d posts loaded", subreddit, subreddit_loaded
            )
            total_loaded += subreddit_loaded

        logger.info("Pipeline complete — %d total posts loaded", total_loaded)

    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
