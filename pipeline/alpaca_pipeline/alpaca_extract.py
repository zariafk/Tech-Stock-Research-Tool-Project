"""Extract stock bar and snapshot data from Alpaca and return pandas DataFrames."""

import os
import json
from datetime import datetime, timezone
import pandas as pd
import requests
from dotenv import load_dotenv
from top_100_tech_companies import tech_universe
from logger import logger
load_dotenv()


BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
SNAPSHOT_URL = "https://data.alpaca.markets/v2/stocks/snapshots"
API_KEY = os.environ["ALPACA_API_KEY"]
API_SECRET = os.environ["ALPACA_API_SECRET"]


def get_ingestion_time() -> str:
    """Get the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def get_request_headers() -> dict[str, str]:
    """Build the request headers for Alpaca authentication."""
    headers = {
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET}
    return headers


def make_request(url: str, headers: dict, params: dict) -> dict:
    """Send a GET request to Alpaca and return the JSON response."""
    headers = get_request_headers()
    try:
        response = requests.get(url, headers=headers,
                                params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

    except requests.RequestException:
        logger.exception(
            "HTTP request failed for URL %s with params %s", url, params)
        raise

    return data


def validate_symbols(symbols: list[str], table_name: str) -> None:
    """Validate that the symbol list is not empty."""

    if not symbols:
        logger.error("Symbol list is empty. Cannot extract %s", table_name)
        raise ValueError("symbols must not be empty")


def build_bars_params(symbols: list[str], start: str, end: str) -> dict:
    """Build query parameters for the daily bars endpoint."""
    params = {
        "symbols": ",".join(symbols),
        "timeframe": "1Day",
        "start": start,
        "end": end,
        "limit": 10000,
        "adjustment": "raw",
        "feed": "iex"}

    return params


def build_snapshot_params(symbols: list[str]) -> dict:
    """Build query parameters for the snapshot endpoint."""
    params = {
        "symbols": ",".join(symbols),
        "feed": "iex"}

    return params


def parse_bar_row(symbol: str, bar: dict, ingestion_timestamp: str) -> dict:
    """Convert one Alpaca bar record into the fact_stock_bars row format."""
    bar_timestamp = bar.get("t")

    row = {
        "symbol": symbol,
        "bar_timestamp": bar_timestamp,
        "bar_date": bar_timestamp,
        "open": bar.get("o"),
        "high": bar.get("h"),
        "low": bar.get("l"),
        "close": bar.get("c"),
        "volume": bar.get("v"),
        "trade_count": bar.get("n"),
        "vwap": bar.get("vw"),
        "ingestion_time": ingestion_timestamp[:19]  # Only to seconds
    }
    return row


def parse_snapshot_row(symbol: str, snapshot: dict, ingestion_timestamp: str) -> dict:
    """Convert one Alpaca snapshot record into the fact_stock_snapshot row format."""
    latest_trade = snapshot.get("latestTrade", {})
    daily_bar = snapshot.get("dailyBar", {})
    prev_daily_bar = snapshot.get("prevDailyBar", {})

    row = {
        "symbol": symbol,
        "snapshot_time": latest_trade.get("t")[:19],
        "latest_trade_price": latest_trade.get("p"),
        "previous_close": prev_daily_bar.get("c"),
        "daily_open": daily_bar.get("o"),
        "daily_high": daily_bar.get("h"),
        "daily_low": daily_bar.get("l"),
        "daily_volume": daily_bar.get("v"),
        "daily_vwap": daily_bar.get("vw"),
        "daily_trade_count": daily_bar.get("n"),
        "ingestion_time": ingestion_timestamp[:19]  # Only to seconds
    }

    return row


def extract_bar_rows_from_response(data: dict, ingestion_timestamp: str) -> list[dict]:
    """Turn one Alpaca bars API response page into a list of row dictionaries."""
    page_rows = []

    page = data.get("bars", {}).items()
    # Loop through each symbol returned on this page
    for symbol, symbol_bars in page:
        # Each bar becomes one output row
        for bar in symbol_bars:
            row = parse_bar_row(symbol, bar, ingestion_timestamp)
            page_rows.append(row)

    return page_rows


def extract_fact_daily_stock_bars(symbols: list[str], start: str, end: str) -> list[dict]:
    """Extract daily historical stock bars for the given symbols and date range.
    One output row represents one symbol on one trading day."""

    validate_symbols(symbols, "fact_stock_bars")

    logger.info(
        "Starting daily stock bar extraction for %s symbols from %s to %s",
        len(symbols), start, end)

    url = BARS_URL
    headers = get_request_headers()
    params = build_bars_params(symbols, start, end)
    ingestion_timestamp = get_ingestion_time()

    all_rows = []
    next_page_token = None
    page_number = 1

    try:
        while True:
            # Add the next page token if Alpaca tells us there is another page
            if next_page_token is not None:
                params["page_token"] = next_page_token
            else:
                params.pop("page_token", None)

            logger.info("Requesting daily bars page %s", page_number)
            data = make_request(url, headers, params)

            # Convert this page of API results into output rows
            page_rows = extract_bar_rows_from_response(
                data, ingestion_timestamp)
            all_rows.extend(page_rows)

            next_page_token = data.get("next_page_token")

            logger.info(
                "Finished page %s. Running total rows extracted: %s", page_number, len(all_rows))

            # Stop once Alpaca no longer provides another page token
            if next_page_token is None:
                break

            page_number = page_number + 1

    except requests.RequestException:
        logger.exception("Daily stock bar extraction failed for %s symbols between %s and %s",
                         len(symbols), start, end)
        raise

    logger.info(
        "Finished daily stock bar extraction. Total rows extracted: %s", len(all_rows))

    return all_rows


def extract_fact_stock_snapshot(symbols: list[str]) -> list[dict]:
    """
    Extract current stock snapshots for the given symbols.
    One output row represents one symbol at one snapshot refresh time.
    """
    validate_symbols(symbols, "fact_stock_snapshot")

    logger.info("Starting stock snapshot extraction for %s stocks", len(symbols))

    url = SNAPSHOT_URL
    headers = get_request_headers()
    params = build_snapshot_params(symbols)
    ingestion_timestamp = get_ingestion_time()

    all_rows = []

    try:
        data = make_request(url, headers, params)

        for symbol in data:
            snapshot = data[symbol]
            row = parse_snapshot_row(symbol, snapshot, ingestion_timestamp)
            all_rows.append(row)

    except requests.RequestException:
        logger.exception(
            "Stock snapshot extraction failed for %s symbols", len(symbols))
        raise

    logger.info(
        "Finished stock snapshot extraction. Total rows extracted: %s", len(all_rows))
    return all_rows


def rows_to_dataframe(rows: list[dict], table_name: str) -> pd.DataFrame:
    """Convert extracted rows into a pandas DataFrame."""
    df = pd.DataFrame(rows)
    logger.info("Converted %s rows into DataFrame for %s", len(df), table_name)
    return df


def extract_all_stock_data(symbols: list[str], start: str, end: str) -> dict:
    """Run the full extraction workflow for both stock bars and snapshots, returning results a
     as dict for rag and dataFrames for transformation"""

    logger.info("Starting stock data extraction workflow")

    try:
        fact_stock_bars_rows = extract_fact_daily_stock_bars(
            symbols, start, end)
        fact_stock_snapshot_rows = extract_fact_stock_snapshot(symbols)

        rag_dicts = {
            "fact_stock_bars": fact_stock_bars_rows,
            "fact_stock_snapshot": fact_stock_snapshot_rows}

        fact_stock_bars_df = rows_to_dataframe(
            fact_stock_bars_rows, "fact_stock_bars")

        fact_stock_snapshot_df = rows_to_dataframe(
            fact_stock_snapshot_rows, "fact_stock_snapshot")

    except requests.RequestException:
        logger.exception("Stock data extraction workflow failed")
        raise

    output = {
        "rag_dict": rag_dicts,  # Include raw extracted dicts for RAG use
        "dataframes": {
            "fact_stock_bars": fact_stock_bars_df,
            # Include DataFrames for transformation use
            "fact_stock_snapshot": fact_stock_snapshot_df}}

    logger.info("Finished stock data extraction workflow")
    return output


# Purely for local testing of extract script, will be removed
# in final version since extract will be called from transform in production
if __name__ == "__main__":
    top_100_symbols = tech_universe

    try:
        extracted_data = extract_all_stock_data(
            symbols=top_100_symbols,
            start="2026-01-01",
            end="2026-03-24"
        )

        print(json.dumps(extracted_data["rag_dict"]
              ["fact_stock_snapshot"][0:2], indent=4))

        print(json.dumps(extracted_data["rag_dict"]
              ["fact_stock_bars"][0:2], indent=4))

        logger.info(
            "Bars rows extracted: %s",
            len(extracted_data["dataframes"]["fact_stock_bars"]))

        print("\nFACT STOCK BARS")
        print(extracted_data["dataframes"]["fact_stock_bars"].head())

        print("\nFACT STOCK SNAPSHOT")
        print(extracted_data["dataframes"]["fact_stock_snapshot"].head())

        logger.info(
            "Bars rows extracted: %s",
            len(extracted_data["dataframes"]["fact_stock_bars"]))

        logger.info(
            "Snapshot rows extracted: %s",
            len(extracted_data["dataframes"]["fact_stock_snapshot"])
        )

    except requests.RequestException:
        logger.exception("Script execution failed")
        raise
