"""RSS Pipeline: Extract, Transform, Load (ETL) for Tech News Articles"""
import json

from rss_analysis import analysis
from rss_transform import transform
from rss_load import load
from logger import logger
from rss_extract_live import (
    extract_live,
    RSS_FEEDS
)


def run_rss_pipeline():
    """Main entry point for the RSS pipeline. Orchestrates the extract, transform, and load steps."""

    logger.info("Starting RSS pipeline run.")

    # === Extract ===
    live = extract_live(RSS_FEEDS)
    rss_articles = analysis(live)

    # === Transform ===
    rss_articles = transform(rss_articles)

    # === Load ===
    net_new = load(rss_articles)
    logger.info("RSS pipeline run complete. %s net new articles loaded.",
                net_new)


def lambda_handler(event, context):
    """ Entry point for AWS Lambda where the pipeline can be triggered by an event."""
    try:
        print("Starting the pipeline execution...")
        run_rss_pipeline()
        return {
            'statusCode': 200,
            'body': json.dumps('Pipeline executed successfully!')
        }
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Pipeline execution failed.')
        }


if __name__ == "__main__":
    run_rss_pipeline()
