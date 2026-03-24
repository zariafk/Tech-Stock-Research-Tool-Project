"""Extract script for RSS pipeline.

Currently fetches news for a predefined list of tech stock tickers from Yahoo Finance RSS feeds."""

from typing import Optional

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


def fetches_feed(ticker: str) -> Optional[feedparser.FeedParserDict]:
    """
    Fetch and parse RSS feed for a given ticker symbol.
    Ticker eg 'AAPL' for Apple Inc.
    Returns feed object is successful, None if error occurs.
    """

    url = YAHOO_FINANCE_RSS_URL.format(ticker=ticker)
    logger.info(f'Fetching RSS feed for {ticker} from: {url}')
    try:
        # feedparser handles HTTP requests and XML parsing internally
        feed = feedparser.parse(url)
        # Check if feedparser encountered a parsing error
        if feed.bozo and feed.bozo_exception:
            logger.warning(
                f'Feed for {ticker} may be malformed: {feed.bozo_exception}'
            )
        # Check HTTP status code if available
        if hasattr(feed, 'status') and feed.status != 200:
            logger.error(
                f'HTTP error {feed.status} when fetching feed for {ticker}'
            )
            return None
        # Verify the feed contains entries
        if not feed.entries:
            logger.warning(f'No entries found in RSS feed for {ticker}')
            return feed

        logger.info(
            f'Successfully fetched {len(feed.entries)} articles for {ticker}'
        )
        return feed
    except Exception as e:
        logger.error(
            f"Exception occurred while fetching RSS feed for {ticker}: {e}")
        return None


def processes_feeds(feeds: list[feedparser.FeedParserDict]) -> list[dict]:
    ...


def extract():
    """Extracts RSS feeds."""
    logger.info("Starting RSS feed extraction.")
    feeds = fetches_feed()
    processed_feeds = processes_feeds(feeds)
    logger.info("Completed RSS feed extraction.")
    return processed_feeds


if __name__ == "__main__":
    extract()
