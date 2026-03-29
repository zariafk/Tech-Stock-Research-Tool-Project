"""Alpaca ETL pipeline — extract, clean, derive, and load to RDS."""

import pandas as pd
import requests
import psycopg2

from alpaca_extract import extract_all_stock_data
from alpaca_transform_cleaning import clean_all_stock_data
from alpaca_load import load_all_to_rds
from top_100_tech_companies import tech_universe
from logger import logger

HISTORY_START_DATE = "2024-01-01"


def get_end_date() -> str:
    """Return today's date in ISO format (UTC)."""
    return pd.Timestamp.now("UTC").date().isoformat()


def extract(symbols: list[str], start: str, end: str) -> dict:
    """Step 1 — pull raw data from the Alpaca API."""
    try:
        logger.info("Step 1: Extracting data from Alpaca API")
        extracted = extract_all_stock_data(symbols, start, end)
        logger.info(
            "Extraction complete — history rows: %d, live rows: %d",
            len(extracted["dataframes"]["alpaca_history"]),
            len(extracted["dataframes"]["alpaca_live"]),
        )
        return extracted

    except requests.RequestException as error:
        logger.exception("Pipeline failed at extraction: %s", error)
        raise


def transform(extracted_output: dict, symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Step 2 — clean and validate the extracted data."""
    try:
        logger.info("Step 2: Cleaning and validating extracted data")
        cleaned = clean_all_stock_data(extracted_output, symbols)

        raw_history = len(extracted_output["dataframes"]["alpaca_history"])
        raw_live = len(extracted_output["dataframes"]["alpaca_live"])
        logger.info(
            "Cleaning complete — history rows: %d (dropped %d), "
            "live rows: %d (dropped %d)",
            len(cleaned["alpaca_history"]),
            raw_history - len(cleaned["alpaca_history"]),
            len(cleaned["alpaca_live"]),
            raw_live - len(cleaned["alpaca_live"]),
        )
        return cleaned

    except ValueError as error:
        logger.exception("Pipeline failed at cleaning: %s", error)
        raise


def load(cleaned_data: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Step 3 — load cleaned data into the RDS."""
    try:
        logger.info("Step 3: Loading cleaned data to RDS")
        result = load_all_to_rds(cleaned_data)
        logger.info(
            "Load complete — history inserted: %d, live inserted: %d",
            result["history_rows_inserted"],
            result["live_rows_inserted"],
        )
        return result

    except (psycopg2.Error, ValueError) as error:
        logger.exception("Pipeline failed at RDS load: %s", error)
        raise


def run_pipeline() -> dict:
    """Execute the full Alpaca ETL pipeline."""
    symbols = list(tech_universe.keys())
    start, end = HISTORY_START_DATE, get_end_date()

    logger.info("Pipeline started — %d symbols, date range %s to %s",
                len(symbols), start, end)

    extracted = extract(symbols, start, end)
    cleaned = transform(extracted, symbols)
    load_result = load(cleaned)

    summary = {
        "status": "success",
        "symbols": len(symbols),
        "date_range": {"start": start, "end": end},
        "extraction": {
            "history_raw": len(extracted["dataframes"]["alpaca_history"]),
            "live_raw": len(extracted["dataframes"]["alpaca_live"]),
        },
        "cleaning": {
            "history_clean": len(cleaned["alpaca_history"]),
            "live_clean": len(cleaned["alpaca_live"]),
        },
        "load": load_result,
    }

    logger.info("Pipeline finished successfully: %s", summary)
    return summary


def lambda_handler(event, context):
    """AWS Lambda entry point for the Alpaca ETL pipeline."""
    logger.info("Lambda invocation started — event: %s", event)

    try:
        return {"statusCode": 200, "body": run_pipeline()}

    except Exception as error:
        logger.exception("Lambda pipeline run failed: %s", error)
        return {
            "statusCode": 500,
            "body": {"status": "error", "error": str(error)},
        }


if __name__ == "__main__":
    run_pipeline()
