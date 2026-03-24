"""Extract script for RSS pipeline.

Currently fetches news for a predefined list of tech stock tickers from Yahoo Finance RSS feeds."""

import feedparser
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)

# List of tech stock tickers to fetch news for
TECH_TICKERS = [
    'AAPL',   # Apple Inc.
    'MSFT',   # Microsoft Corporation
    'NVDA',   # NVIDIA Corporation
    'GOOGL',  # Alphabet Inc.
    'AMZN',   # Amazon.com Inc.
    'META',   # Meta Platforms Inc.
    'TSLA',   # Tesla Inc.
    'AMD',    # Advanced Micro Devices
    'INTC',   # Intel Corporation
    'CRM',    # Salesforce Inc.
]

# Base URL template for Yahoo Finance RSS feeds
YAHOO_FINANCE_RSS_URL = 'https://finance.yahoo.com/rss/headline?s={ticker}'


def fetches_feed():


def processes_feeds():


def extract():
    """Extracts RSS feeds."""
    logger.info("Starting RSS feed extraction.")
    feeds = fetches_feed()
    processed_feeds = processes_feeds(feeds)
    logger.info("Completed RSS feed extraction.")
    return processed_feeds


if __name__ == "__main__":
    extract()
