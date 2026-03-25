"""Extract script for RSS pipeline.

Fetches news for tech stock tickers from
TechCrunch and Hacker News, 
filters by relevance using OpenAI.
"""

import logging
from typing import Optional
from datetime import datetime
import pandas as pd
import feedparser
import time
from openai import OpenAI
# from pipeline.logger import make_logger
import requests
import ssl
import dotenv
import os
# import spacy

dotenv.load_dotenv()

logger = logging.getLogger(__name__)

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

TECH_TICKERS = [
    'AAPL',
    'MSFT',
    'NVDA',
    'GOOGL',
    'AMZN',
    'META',
    'TSLA',
    'AMD',
    'INTC',
    'CRM',
]

TICKER_COMPANIES = {
    'AAPL': 'Apple',
    'MSFT': 'Microsoft',
    'NVDA': 'NVIDIA',
    'GOOGL': 'Google',
    'AMZN': 'Amazon',
    'META': 'Meta',
    'TSLA': 'Tesla',
    'AMD': 'AMD',
    'INTC': 'Intel',
    'CRM': 'Salesforce',
}

RSS_FEEDS = {
    'techcrunch': 'https://techcrunch.com/feed/'
    # 'hackernews': 'https://hnrss.org/frontpage?points=100'
}

CLIENT = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


# Testing with spacy for entity extraction, or hybrid approach with OpenAI for better accuracy.
# nlp = spacy.load(
#     "en_core_web_sm"
# )


def fetch_feed(url: str) -> Optional[
    feedparser.FeedParserDict
]:
    """Fetch RSS feed, bypass SSL
    verification for Docker."""
    logger.info(
        'Fetching RSS feed: %s', url
    )

    try:
        response = requests.get(
            url,
            verify=False,  # skip SSL verification
            timeout=10
        )
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(
            'Network error: %s', e
        )
        return None

    feed = feedparser.parse(
        response.content
    )

    if feed.bozo and (
        feed.bozo_exception
    ):
        logger.warning(
            'Feed malformed: %s',
            feed.bozo_exception
        )

    if not feed.entries:
        logger.warning(
            'No entries in feed'
        )
        return None

    logger.info(
        'Fetched %d articles',
        len(feed.entries)
    )
    return feed


def extract_entry_fields(entry: feedparser.FeedParserDict, source: str,) -> dict:
    """Extract fields from RSS entry."""
    title = entry.get('title', 'N/A')
    link = entry.get('link', 'N/A')
    summary = entry.get(
        'summary',
        entry.get('description', 'N/A')
    )

    # Parse published date or use fallback
    published_date = 'N/A'
    if hasattr(entry, 'published_parsed'):
        if entry.published_parsed:
            try:
                pub_dt = datetime(
                    *entry.published_parsed[:6]
                )
                published_date = (
                    pub_dt.strftime(
                        '%Y-%m-%d %H:%M:%S'
                    )
                )
            except (TypeError, ValueError):
                logger.debug(
                    'Could not parse date: %s',
                    title
                )
    elif 'published' in entry:
        published_date = entry.published

    return {
        'title': title,
        'link': link,
        'summary': summary,
        'published_date': published_date,
        'source': source,
    }


def format_ticker_prompt(entry: dict, tickers: list[str]) -> str:
    """Format OpenAI prompt for ticker matching."""
    ticker_str = ', '.join(tickers)
    companies = list(TICKER_COMPANIES[t] for t in tickers)
    company_str = ', '.join(companies)

    return (
        'Which of the following tickers is this article about? '
        'Companies: %s Tickers: %s '
        'Title: %s Summary: %s '
        'Return only tickers comma-sep. '
        'Return "NONE" if no match.'
    ) % (company_str, ticker_str,
         entry['title'], entry['summary'])


def get_relevant_tickers(entry: dict, tickers: list[str]) -> list[str]:
    """
    Get tickers relevant to article.
    Returns list of matching tickers, empty if none match.
    """
    prompt = format_ticker_prompt(
        entry, tickers
    )

    response = (
        CLIENT.chat.completions.create(
            model='gpt-4o-mini',
            messages=[{
                'role': 'user',
                'content': prompt
            }]
        )
    )
    result = (
        response
        .choices[0]
        .message
        .content
        .strip()
        .upper()
    )

    if result == 'NONE':
        return []

    matched = result.split(',')
    return [ticker.strip() for ticker in matched]


def log_summary(articles: list) -> None:
    """Log extraction summary."""
    logger.info('=' * 50)
    logger.info('Extraction Summary:')
    logger.info(
        '  Total articles extracted: %d',
        len(articles)
    )
    logger.info('=' * 50)


def create_dataframe(articles: list) -> pd.DataFrame:
    """Create DataFrame from articles."""
    if not articles:
        logger.warning(
            'No articles extracted. '
            'Returning empty DataFrame.'
        )
        return pd.DataFrame(
            columns=[
                'ticker', 'title', 'link',
                'summary', 'published_date',
                'source'
            ]
        )

    df = pd.DataFrame(articles)
    column_order = [
        'ticker', 'title', 'link',
        'summary', 'published_date', 'source'
    ]
    df = df[column_order]

    # Coerce dates to datetime
    df['published_date'] = pd.to_datetime(
        df['published_date'], errors='coerce'
    )

    # Sort by ticker and date
    df = df.sort_values(
        by=['ticker', 'published_date'],
        ascending=[True, False]
    ).reset_index(drop=True)

    logger.info('Final DataFrame shape: %s',
                df.shape)
    return df


def extract(tickers: list[str] = None) -> pd.DataFrame:
    """Extract articles for tickers from RSS feeds, filter with OpenAI."""
    if tickers is None:
        tickers = TECH_TICKERS

    logger.info(
        'Starting extraction for %d tickers.',
        len(tickers)
    )

    all_articles = []

    # Process each RSS feed source
    for source, url in RSS_FEEDS.items():
        logger.info(
            'Processing source: %s', source
        )
        feed = fetch_feed(url)
        time.sleep(2)

        if feed is None:
            continue

        # Check each article get relevant tickers
        for entry in feed.entries:
            article = (
                extract_entry_fields(
                    entry, source
                )
            )
            matched_tickers = get_relevant_tickers(
                article, tickers
            )
            time.sleep(1)
            for ticker in matched_tickers:
                article_copy = article.copy()
                article_copy['ticker'] = ticker
                all_articles.append(article_copy)

    log_summary(all_articles)

    return create_dataframe(all_articles)


if __name__ == '__main__':
    df = extract()
    print(df.head())
