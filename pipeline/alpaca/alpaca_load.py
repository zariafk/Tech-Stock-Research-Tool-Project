"""Load cleaned Alpaca stock data into an RDS instance."""

import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from logger import logger

load_dotenv()

HISTORY_COLUMNS = [
    "stock_id", "bar_date", "open", "high", "low",
    "close", "volume", "trade_count", "vwap",]

LIVE_COLUMNS = [
    "stock_id", "latest_time", "open", "high", "low",
    "close", "volume", "trade_count", "vwap",]


def get_rds_connection():
    """Open a new psycopg2 connection using credentials from the environment."""
    connection = psycopg2.connect(
        host=os.environ["RDS_HOST"], port=os.environ.get("RDS_PORT", "5432"),
        dbname=os.environ["RDS_DB"], user=os.environ["RDS_USER"],
        password=os.environ["RDS_PASSWORD"])

    return connection


def fetch_stock_id_map(connection) -> dict[str, int]:
    """Return a dict mapping ticker → stock_id from the stock table.
    Raises ValueError when the stock table is empty."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT stock_id, ticker FROM stock;")
        rows = cursor.fetchall()

    if not rows:
        raise ValueError("The stock table is empty — populate it first")

    stock_id_map = {ticker: stock_id for stock_id, ticker in rows}
    logger.info("Loaded %d stock_id mappings from the stock table",
                len(stock_id_map))
    return stock_id_map


def map_ticker_to_stock_id(df: pd.DataFrame,
                           stock_id_map: dict[str, int]) -> pd.DataFrame:
    """Replace the ticker column with the corresponding stock_id.

    Rows whose ticker cannot be found in the map are dropped with a
    warning so the rest of the batch can still be loaded.
    """
    mapped_df = df.copy()
    mapped_df["stock_id"] = mapped_df["ticker"].map(stock_id_map)

    unmapped_tickers = (
        mapped_df.loc[mapped_df["stock_id"].isna(), "ticker"].unique().tolist()
    )
    if unmapped_tickers:
        logger.warning(
            "Dropping %d ticker(s) not found in stock table: %s",
            len(unmapped_tickers), unmapped_tickers,
        )
        mapped_df = mapped_df.dropna(subset=["stock_id"]).copy()

    mapped_df["stock_id"] = mapped_df["stock_id"].astype(int)
    return mapped_df.drop(columns=["ticker"])


def fetch_existing_history_range(
        connection) -> dict[int, tuple[datetime, datetime]]:
    """Return a dict of stock_id → (min_date, max_date) from alpaca_history."""
    query = """
        SELECT stock_id, MIN(bar_date) AS earliest_date, MAX(bar_date) AS latest_date
        FROM alpaca_history
        GROUP BY stock_id;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    date_ranges = {stock_id: (earliest, latest)
                   for stock_id, earliest, latest in rows}
    logger.info(
        "Found existing history for %d stock(s) in alpaca_history", len(date_ranges))
    return date_ranges


def filter_new_history_rows(df: pd.DataFrame,
                            date_ranges: dict[int, tuple[datetime, datetime]]) -> pd.DataFrame:
    """Keep only history rows not already covered by the RDS date range.
    On first run (empty date_ranges or stock_id not in map) all rows
    for that stock are included.
    """
    if not date_ranges:
        logger.info("No existing history found — loading full history")
        return df.copy()

    keep_mask = pd.Series(True, index=df.index)

    for stock_id, (earliest_date, latest_date) in date_ranges.items():
        stock_rows = df["stock_id"] == stock_id
        earliest = earliest_date.date() if isinstance(
            earliest_date, datetime) else earliest_date
        latest = latest_date.date() if isinstance(
            latest_date, datetime) else latest_date

        bar_dates = pd.to_datetime(df.loc[stock_rows, "bar_date"]).dt.date
        keep_mask.loc[stock_rows] = (
            bar_dates > latest) | (bar_dates < earliest)

    filtered_df = df.loc[keep_mask].copy()
    logger.info("Incremental filter: %d of %d history rows are new",
                len(filtered_df), len(df))
    return filtered_df


def insert_history_rows(connection, df: pd.DataFrame) -> int:
    """Bulk-insert rows into alpaca_history using execute_values."""
    if df.empty:
        logger.info("No new history rows to insert")
        return 0

    insert_sql = """
        INSERT INTO alpaca_history
            (stock_id, bar_date, open, high, low, close, volume, trade_count, vwap)
        VALUES %s;
    """
    rows_to_insert = df[HISTORY_COLUMNS].copy()
    rows_to_insert["bar_date"] = pd.to_datetime(
        rows_to_insert["bar_date"]).dt.date
    tuples = [tuple(row) for row in rows_to_insert.itertuples(index=False)]

    with connection.cursor() as cursor:
        execute_values(cursor, insert_sql, tuples, page_size=1000)
    connection.commit()

    logger.info("Inserted %d rows into alpaca_history", len(tuples))
    return len(tuples)


def load_alpaca_history(connection, cleaned_history_df: pd.DataFrame,
                        stock_id_map: dict[str, int]) -> int:
    """Full incremental load workflow for the alpaca_history table."""

    logger.info("Starting alpaca_history load")
    mapped_df = map_ticker_to_stock_id(cleaned_history_df, stock_id_map)

    if mapped_df.empty:
        logger.warning("No mapped history rows — nothing to load")
        return 0

    date_ranges = fetch_existing_history_range(connection)
    new_rows_df = filter_new_history_rows(mapped_df, date_ranges)
    rows_inserted = insert_history_rows(connection, new_rows_df)

    logger.info("Finished alpaca_history load. Rows inserted: %d", rows_inserted)
    return rows_inserted


def get_live_window_start() -> datetime:
    """Return the UTC timestamp of the most recent 12:00 UK noon.

    The live table holds data for a single trading window that resets
    every day at 12:00 Europe/London.  If it is currently before noon
    UK time the window started at yesterday's noon; otherwise it started
    at today's noon.
    """
    now_uk = datetime.now(ZoneInfo("Europe/London"))
    noon_today_uk = now_uk.replace(hour=12, minute=0, second=0, microsecond=0)
    window_start_uk = noon_today_uk - \
        timedelta(days=1) if now_uk < noon_today_uk else noon_today_uk
    return window_start_uk.astimezone(timezone.utc)


def delete_stale_live_rows(connection) -> int:
    """Delete all alpaca_live rows that fall before the current live window.

    The live window resets every day at 12:00 UK time.  Any rows with
    latest_time before that cutoff belong to a previous session and are
    removed so only the current day's data remains.
    """
    window_start_utc = get_live_window_start()

    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM alpaca_live WHERE latest_time < %s;", (window_start_utc,))
        rows_deleted = cursor.rowcount
    connection.commit()

    logger.info("Deleted %d stale live rows (window start: %s)",
                rows_deleted, window_start_utc)
    return rows_deleted


def fetch_existing_live_keys(connection) -> set[tuple[int, datetime]]:
    """Return a set of (stock_id, latest_time) already in alpaca_live."""

    with connection.cursor() as cursor:
        cursor.execute("SELECT stock_id, latest_time FROM alpaca_live;")
        rows = cursor.fetchall()

    existing_keys = set(rows)
    logger.info("Found %d existing row(s) in alpaca_live", len(existing_keys))
    return existing_keys


def filter_new_live_rows(df: pd.DataFrame,
                         existing_keys: set[tuple[int, datetime]]) -> pd.DataFrame:
    """Keep only live rows whose (stock_id, latest_time) is not already in the RDS."""

    if not existing_keys:
        return df.copy()

    latest_times = pd.to_datetime(df["latest_time"], utc=True)
    is_new = [
        (row.stock_id, ts) not in existing_keys
        for row, ts in zip(df.itertuples(), latest_times)
    ]
    new_df = df.loc[is_new].copy()

    skipped = len(df) - len(new_df)
    if skipped > 0:
        logger.info(
            "Skipping %d live row(s) already present in alpaca_live", skipped)
    return new_df


def insert_live_rows(connection, df: pd.DataFrame) -> int:
    """Bulk-insert rows into alpaca_live using execute_values."""
    if df.empty:
        logger.info("No new live rows to insert")
        return 0

    insert_sql = """
        INSERT INTO alpaca_live
            (stock_id, latest_time, open, high, low, close, volume, trade_count, vwap)
        VALUES %s;
    """
    tuples = [tuple(row) for row in df[LIVE_COLUMNS].itertuples(index=False)]

    with connection.cursor() as cursor:
        execute_values(cursor, insert_sql, tuples, page_size=1000)
    connection.commit()

    logger.info("Inserted %d rows into alpaca_live", len(tuples))
    return len(tuples)


def load_alpaca_live(connection, cleaned_live_df: pd.DataFrame,
                     stock_id_map: dict[str, int]) -> int:
    """Full daily-reset load workflow for the alpaca_live table."""

    logger.info("Starting alpaca_live load")
    mapped_df = map_ticker_to_stock_id(cleaned_live_df, stock_id_map)

    if mapped_df.empty:
        logger.warning("No mapped live rows — nothing to load")
        return 0

    # 1. Remove previous-day rows
    delete_stale_live_rows(connection)
    existing_keys = fetch_existing_live_keys(
        connection)     # 2. Fetch already-loaded keys
    mapped_df = filter_new_live_rows(mapped_df, existing_keys)
    rows_inserted = insert_live_rows(connection, mapped_df)

    logger.info("Finished alpaca_live load. Rows inserted: %d", rows_inserted)
    return rows_inserted


def load_all_to_rds(cleaned_data: dict[str, pd.DataFrame],
                    connection=None) -> dict[str, int]:
    """Load both alpaca_history and alpaca_live into the RDS."""
    own_connection = connection is None

    try:
        logger.info("Starting full Alpaca RDS load workflow")
        if own_connection:
            connection = get_rds_connection()

        stock_id_map = fetch_stock_id_map(connection)
        history_inserted = load_alpaca_history(
            connection, cleaned_data["alpaca_history"], stock_id_map)
        live_inserted = load_alpaca_live(
            connection, cleaned_data["alpaca_live"], stock_id_map)

        logger.info(
            "Finished full Alpaca RDS load workflow. History rows: %d, Live rows: %d",
            history_inserted, live_inserted,
        )
        return {"history_rows_inserted": history_inserted, "live_rows_inserted": live_inserted}

    except Exception as error:
        logger.exception("Alpaca RDS load workflow failed: %s", error)
        raise

    finally:
        if own_connection and connection is not None:
            connection.close()
            logger.info("RDS connection closed")
