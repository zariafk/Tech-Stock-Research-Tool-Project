"""Alpaca ETL pipeline — extract, clean, derive, and load to RDS."""

import pandas as pd
import requests
import psycopg2
from dotenv import load_dotenv

from alpaca_extract import extract_all_stock_data
from alpaca_transform_cleaning import clean_all_stock_data
from alpaca_load import load_all_to_rds
from top_100_tech_companies import tech_universe
from logger import logger

load_dotenv()

HISTORY_START_DATE = "2024-01-01"


def get_end_date():
    """Return today's date in ISO format (UTC)."""
    return pd.Timestamp.now("UTC").date().isoformat()


def run_pipeline():
    """Execute the full Alpaca ETL pipeline """

    symbols = list(tech_universe.keys())
    start = HISTORY_START_DATE
    end = get_end_date()

    logger.info(
        "Pipeline started — %d symbols, date range %s to %s",
        len(symbols), start, end,)

    # EXTRACTION

    try:
        logger.info("Step 1/3: Extracting data from Alpaca API")
        extracted_output = extract_all_stock_data(symbols, start, end)

        history_raw_count = len(
            extracted_output["dataframes"]["alpaca_history"])
        live_raw_count = len(
            extracted_output["dataframes"]["alpaca_live"])

        logger.info(
            "Extraction complete — history rows: %d, live rows: %d",
            history_raw_count, live_raw_count)

    except requests.RequestException as error:
        logger.exception("Pipeline failed at extraction: %s", error)
        raise

    # TRANSFORM & CLEAN

    try:
        logger.info("Step 2/3: Cleaning and validating extracted data")
        cleaned_data = clean_all_stock_data(extracted_output, symbols)

        history_clean_count = len(cleaned_data["alpaca_history"])
        live_clean_count = len(cleaned_data["alpaca_live"])

        logger.info(
            "Cleaning complete — history rows: %d (dropped %d), "
            "live rows: %d (dropped %d)",
            history_clean_count,
            history_raw_count - history_clean_count,
            live_clean_count,
            live_raw_count - live_clean_count,
        )

    except ValueError as error:
        logger.exception("Pipeline failed at cleaning: %s", error)
        raise

    # LOAD TO RDS

    try:
        logger.info("Step 3/3: Loading cleaned data to RDS")
        load_result = load_all_to_rds(cleaned_data)

        logger.info(
            "Load complete — history inserted: %d, live inserted: %d",
            load_result["history_rows_inserted"],
            load_result["live_rows_inserted"],
        )

    except psycopg2.Error as error:
        logger.exception("Pipeline failed at RDS load: %s", error)
        raise

    except ValueError as error:
        logger.exception("Pipeline failed at RDS load (data issue): %s",
                         error)
        raise

    summary = {
        "status": "success",
        "symbols": len(symbols),
        "date_range": {"start": start, "end": end},
        "extraction": {
            "history_raw": history_raw_count,
            "live_raw": live_raw_count,
        },
        "cleaning": {
            "history_clean": history_clean_count,
            "live_clean": live_clean_count,
        },
        "load": load_result,
    }

    logger.info("Pipeline finished successfully: %s", summary)
    return summary


def lambda_handler(event, context):
    """AWS Lambda entry point for the Alpaca ETL pipeline"""
    logger.info("Lambda invocation started — event: %s", event)

    try:
        summary = run_pipeline()

        return {
            "statusCode": 200,
            "body": summary}

    except Exception as error:
        logger.exception("Lambda pipeline run failed: %s", error)

        return {
            "statusCode": 500,
            "body": {
                "status": "error",
                "error": str(error),
            },
        }


if __name__ == "__main__":
    run_pipeline()
