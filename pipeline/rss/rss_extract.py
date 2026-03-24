"""Extract script for RSS pipeline.

Currently fetches news for a predefined list of tech stock tickers from Yahoo Finance RSS feeds."""


import logging
from datetime import datetime
from typing import Optional
import requests
import feedparser
import pandas as pd
from logger import make_logger

logger = make_logger()

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


def fetch_rss_feed(ticker: str) -> Optional[feedparser.FeedParserDict]:
    """
    Fetch and parse RSS feed for a given ticker symbol.
    Ticker eg 'AAPL' for Apple Inc.
    Returns feed object is successful, None if error occurs.
    """

    url = YAHOO_FINANCE_RSS_URL.format(ticker=ticker)

    logger.info('Fetching RSS feed for %s from: %s', ticker, url)

    # Use requests to fetch the feed first to handle network errors and timeouts
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses
    except requests.exceptions.RequestException as e:
        logger.error(
            "Network error occurred while fetching RSS feed for %s: %s", ticker, e)
        return None

    # Parse the feed content using feedparser
    try:
        # feedparser handles HTTP requests and XML parsing internally
        feed = feedparser.parse(url)
        # Check if feedparser encountered a parsing error
        if feed.bozo and feed.bozo_exception:
            logger.warning(
                'Feed for %s may be malformed: %s', ticker, feed.bozo_exception
            )
        # Check HTTP status code if available
        if hasattr(feed, 'status') and feed.status != 200:
            logger.error(
                'HTTP error %s when fetching feed for %s', feed.status, ticker
            )
            return None
        # Verify the feed contains entries
        if not feed.entries:
            logger.warning('No entries found in RSS feed for %s', ticker)
            return feed

        logger.info(
            'Successfully fetched %d articles for %s', len(
                feed.entries), ticker
        )
        return feed
    except Exception as e:
        logger.error(
            "Exception occurred while fetching RSS feed for %s: %s", ticker, e)
        return None


def extract_article_fields(entry: feedparser.FeedParserDict, ticker: str) -> dict:
    """
    Extracts relevant fields from a single RSS feed entry.
    Returns a dictionary containing extracted article fields.
    """
    # Safely extract title with fallback
    title = entry.get('title', 'N/A')
    # Safely extract article link
    link = entry.get('link', 'N/A')
    # Extract summary/description - clean up any HTML tags if present
    summary = entry.get('summary', entry.get('description', 'N/A'))

    # Extract and format the published date
    published_date = 'N/A'
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        try:
            # Convert time.struct_time to datetime string
            published_date = datetime(
                *entry.published_parsed[:6]
            ).strftime('%Y-%m-%d %H:%M:%S')
        except (TypeError, ValueError) as e:
            logger.debug('Could not parse date for article "%s": %s', title, e)
            published_date = entry.get('published', 'N/A')
    elif 'published' in entry:
        published_date = entry.published

    # Extract source information from tags or feed source
    source = 'Yahoo Finance'
    if hasattr(entry, 'tags') and entry.tags:
        # Some entries include source as a tag
        source_tags = [
            tag.get('term', '') for tag in entry.tags
            if tag.get('scheme', '').endswith('provider')
        ]
        if source_tags:
            source = source_tags[0]

    return {
        'ticker': ticker,
        'title': title,
        'link': link,
        'summary': summary,
        'published_date': published_date,
        'source': source,
    }


def log_summary(successful_tickers, failed_tickers, all_articles):
    """Helper function to log summary statistics after extraction."""
    # Log summary statistics
    logger.info('=' * 50)
    logger.info('Extraction Summary:')
    logger.info(
        '  Successful tickers: %d - %s', len(successful_tickers), successful_tickers)
    logger.info('  Failed tickers: %d - %s',
                len(failed_tickers), failed_tickers)
    logger.info('  Total articles extracted: %d', len(all_articles))
    logger.info('=' * 50)


def create_dataframe(articles: list) -> pd.DataFrame:
    """Helper function to create a pandas DataFrame from a list of article dictionaries."""
    if not articles:
        logger.warning(
            'No articles to create DataFrame. Returning empty DataFrame.')
        return pd.DataFrame(
            columns=['ticker', 'title', 'link',
                     'summary', 'published_date', 'source']
        )

    df = pd.DataFrame(articles)

    # Ensure correct column order
    column_order = ['ticker', 'title', 'link',
                    'summary', 'published_date', 'source']
    df = df[column_order]

    # Convert published_date to datetime where possible for better sorting
    df['published_date'] = pd.to_datetime(
        df['published_date'], errors='coerce'
    )

    # Sort by ticker and published date (most recent first)
    df = df.sort_values(
        by=['ticker', 'published_date'],
        ascending=[True, False]
    ).reset_index(drop=True)

    logger.info('Final DataFrame shape: %s', df.shape)
    return df


def extract():
    """Main extract function for extracting RSS feeds.
    Args:
        tickers: List of stock ticker symbols. Defaults to TECH_TICKERS if None.

    Returns:
        pandas DataFrame with columns: ticker, title, link, summary,
        published_date, source
    """

    if tickers is None:
        tickers = TECH_TICKERS

    logger.info("Starting RSS feed extraction for %s tickers.", len(tickers))

    all_articles = []  # Collect all article dictionaries
    successful_tickers = []
    failed_tickers = []

    for ticker in tickers:
        logger.info("Processing ticker: %s", ticker)
        feed = fetch_rss_feed(ticker)
        if feed is None:
            logger.error("Skipping ticker %s due to fetch error.", ticker)
            failed_tickers.append(ticker)
            continue
        if not feed.entries:
            logger.warning("No articles found for ticker %s.", ticker)
            successful_tickers.append(ticker)
            continue

        #  Extract fields from each article entry
        ticker_articles = []
        for entry in feed.entries:
            try:
                article_data = extract_article_fields(entry, ticker)
                ticker_articles.append(article_data)
            except Exception as e:
                logger.error(
                    "Error extracting article for ticker %s: %s", ticker, e
                )
        all_articles.extend(ticker_articles)
        successful_tickers.append(ticker)
        logger.info(
            "Extracted %d articles for ticker %s.", len(
                ticker_articles), ticker
        )

    log_summary(successful_tickers, failed_tickers, all_articles)

    # Create DataFrame from collected articles
    if not all_articles:
        logger.warning(
            'No articles were extracted. Returning empty DataFrame.')
        return create_dataframe([])

    df = create_dataframe(all_articles)

    return df


if __name__ == "__main__":
    df = extract()
    print(df.head())
