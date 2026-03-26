"""RSS Pipeline: Extract, Transform, Load (ETL) for Tech News Articles"""
from rss_extract import extract
from rss_transform import transform
from rss_load import load
from logger import logger


def run_rss_pipeline():
    """Main entry point for the RSS pipeline. Orchestrates the extract, transform, and load steps."""

    logger.info("Starting RSS pipeline run.")

    # === Extract ===
    rss_articles = extract()

    # === Transform ===
    rss_articles = transform(rss_articles)

    # === Load ===
    net_new = load(rss_articles)
    logger.info("RSS pipeline run complete. %s net new articles loaded.",
                net_new)
