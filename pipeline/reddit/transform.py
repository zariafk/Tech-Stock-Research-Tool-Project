"""Reddit data transform script."""

import logging

import pandas as pd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def flatten_post_data(raw_posts: list[dict]) -> pd.DataFrame:
    """Extracts the nested 'data' dict from each raw post into a flat DataFrame."""
    rows = [post["data"] for post in raw_posts if "data" in post]
    return pd.DataFrame(rows)


def drop_missing_required(
    df: pd.DataFrame,
    required_columns: list[str],
) -> pd.DataFrame:
    """Drops rows where any required column is null or '[deleted]'."""
    before = len(df)
    df = df.dropna(subset=required_columns)

    sentinel_mask = (
        df[required_columns].isin(["[deleted]", "[removed]"]).any(axis=1)
    )
    df = df[~sentinel_mask]

    dropped = before - len(df)
    if dropped:
        logger.info(
            "Dropped %d rows with missing/deleted required fields", dropped)

    return df


def validate_numeric_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """Ensures numeric columns fall within reasonable bounds."""
    df["score"] = df["score"].clip(lower=0)
    df["ups"] = df["ups"].clip(lower=0)
    df["num_comments"] = df["num_comments"].clip(lower=0)
    df["upvote_ratio"] = df["upvote_ratio"].clip(lower=0.0, upper=1.0)
    df["subreddit_subscribers"] = df["subreddit_subscribers"].clip(lower=0)
    return df


def convert_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Converts Unix timestamps to ISO 8601 datetime strings."""
    df["created_utc"] = (
        pd.to_datetime(df["created_utc"], unit="s", utc=True)
        .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    return df


def build_fact_posts(
    df: pd.DataFrame,
    fact_columns: list[str],
) -> pd.DataFrame:
    """Selects and deduplicates columns for the FACT_posts table."""
    fact = df[fact_columns].copy()
    fact = fact.drop_duplicates(subset="id")
    logger.info("FACT_posts: %d rows", len(fact))
    return fact


def build_dim_subreddits(
    df: pd.DataFrame,
    dim_columns: list[str],
) -> pd.DataFrame:
    """Builds the DIM_subreddits table, keeping one row per subreddit."""
    dim = df[dim_columns].copy()

    dim = (
        dim.sort_values("subreddit_subscribers", ascending=False)
        .drop_duplicates(subset="subreddit_id")
        .reset_index(drop=True)
    )

    dim = dim.rename(columns={"subreddit": "subreddit_name"})

    logger.info("DIM_subreddits: %d rows", len(dim))
    return dim


def deduplicate(
    df: pd.DataFrame,
    existing_ids: set[str],
    id_column: str = "id",
) -> pd.DataFrame:
    """Removes rows whose ID already exists in the existing dataset."""
    before = len(df)
    df = df[~df[id_column].isin(existing_ids)]
    removed = before - len(df)

    if removed:
        logger.info("Removed %d duplicate rows, %d new rows remaining",
                    removed, len(df))
    return df


# def transform_main(
#     raw_posts: list[dict],
#     *,
#     fact_columns: list[str],
#     dim_columns: list[str],
#     required_columns: list[str],
# ) -> tuple[pd.DataFrame, pd.DataFrame]:
#     """Runs the full transform pipeline and returns (fact_posts, dim_subreddits)."""
#     if not raw_posts:
#         logger.warning("No posts to transform")
#         empty_fact = pd.DataFrame(columns=fact_columns)
#         empty_dim = pd.DataFrame(columns=dim_columns)
#         return empty_fact, empty_dim

#     df = flatten_post_data(raw_posts)

#     all_columns = list(set(fact_columns + dim_columns))
#     df = df.reindex(columns=all_columns)

#     df = drop_missing_required(df, required_columns)
#     df = validate_numeric_ranges(df)
#     df = convert_timestamps(df)

#     fact_posts = build_fact_posts(df, fact_columns)
#     dim_subreddits = build_dim_subreddits(df, dim_columns)

#     return fact_posts, dim_subreddits


def transform_main(
    raw_posts: list[dict],
    *,
    fact_columns: list[str],
    dim_columns: list[str],
    required_columns: list[str],
    existing_post_ids: set[str] | None = None,
    existing_subreddit_ids: set[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Runs the full transform pipeline and returns (fact_posts, dim_subreddits)."""
    if not raw_posts:
        logger.warning("No posts to transform")
        empty_fact = pd.DataFrame(columns=fact_columns)
        empty_dim = pd.DataFrame(columns=dim_columns)
        return empty_fact, empty_dim

    df = flatten_post_data(raw_posts)

    all_columns = list(set(fact_columns + dim_columns))
    df = df.reindex(columns=all_columns)

    df = drop_missing_required(df, required_columns)
    df = validate_numeric_ranges(df)
    df = convert_timestamps(df)

    fact_posts = build_fact_posts(df, fact_columns)
    dim_subreddits = build_dim_subreddits(df, dim_columns)

    # Deduplicate against existing bucket data if provided
    if existing_post_ids is not None:
        fact_posts = deduplicate(fact_posts, existing_post_ids, "id")
    if existing_subreddit_ids is not None:
        dim_subreddits = deduplicate(
            dim_subreddits, existing_subreddit_ids, "subreddit_id")

    return fact_posts, dim_subreddits

# mappings = load_mappings()
# tickers = mappings["symbol"].tolist()
# print(tickers)
