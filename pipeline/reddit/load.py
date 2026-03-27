"""Reddit data load script — writes DataFrames to a PostgreSQL RDS instance."""

import json
import logging

import boto3
import psycopg2
import psycopg2.extras
import pandas as pd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_secret(secret_name: str, region: str = "eu-west-2") -> dict:
    """Retrieves a secret from AWS Secrets Manager and returns it as a dict."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_connection(secret: dict) -> psycopg2.extensions.connection:
    """Creates a PostgreSQL connection from a secret dict."""
    return psycopg2.connect(
        host=secret["host"],
        port=secret.get("port", 5432),
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
    )


def get_existing_ids(
    conn: psycopg2.extensions.connection,
    table: str,
    id_column: str,
) -> set[str]:
    """Fetches all existing IDs from a table."""
    query = f"SELECT {id_column} FROM {table}"  # noqa: S608

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    ids = {row[0] for row in rows}
    logger.info("Found %d existing IDs in %s", len(ids), table)
    return ids


def insert_dataframe(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    table: str,
) -> None:
    """Inserts a DataFrame into a PostgreSQL table using batch execute."""
    if df.empty:
        logger.warning("Skipping empty DataFrame for table: %s", table)
        return

    columns = list(df.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    column_names = ", ".join(columns)
    query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"  # noqa: S608

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, query, rows)

    conn.commit()
    logger.info("Inserted %d rows into %s", len(rows), table)


def join_tables_to_json(
    fact_posts: pd.DataFrame,
    dim_subreddits: pd.DataFrame,
    fact_post_tickers: pd.DataFrame,
) -> list[dict]:
    """Joins all three tables, nesting ticker analysis per post."""
    joined = fact_posts.merge(dim_subreddits, on="subreddit_id", how="left")
    records = joined.to_dict(orient="records")

    # Group ticker results by post_id for fast lookup
    ticker_cols = ["ticker", "relevance_score", "sentiment", "analysis"]
    ticker_groups = (
        fact_post_tickers.groupby("post_id")[ticker_cols]
        .apply(lambda g: g.to_dict(orient="records"))
        .to_dict()
    )

    for record in records:
        record["tickers"] = ticker_groups.get(record["id"], [])

    return records


def load_main(
    tables: dict[str, pd.DataFrame],
    *,
    conn: psycopg2.extensions.connection,
) -> None:
    """Loads all tables into PostgreSQL."""
    for table_name, df in tables.items():
        if df.empty:
            logger.info("No new rows for %s, skipping", table_name)
            continue

        insert_dataframe(conn, df, table_name)

    logger.info("Load complete — %d tables processed", len(tables))
