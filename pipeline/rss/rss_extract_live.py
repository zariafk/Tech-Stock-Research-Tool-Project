"""Extract live tech news from RSS
feeds."""

import feedparser
import requests
import time
from typing import Optional
from datetime import datetime
from logger import logger


RSS_FEEDS = {
    'techcrunch': 'https://techcrunch.com/feed/'
}


def fetch_feed(url: str) -> Optional[feedparser.FeedParserDict]:
    """Fetch RSS feed."""
    logger.info('LIVE: Fetching RSS: %s', url)

    response = requests.get(url, verify=False, timeout=10)

    if response.status_code != 200:
        logger.error('LIVE: Failed: %s (Status: %s)',
                     url, response.status_code)
        return None

    feed = feedparser.parse(response.content)

    if not feed.entries:
        logger.warning('LIVE: No entries in feed: %s', url)
        return None

    return feed


def extract_entry_fields(entry: feedparser.FeedParserDict, source: str) -> dict:
    """Extract RSS entry fields."""
    title = entry.get('title', 'N/A')
    link = entry.get('link', 'N/A')
    summary = entry.get('summary', entry.get('description', 'N/A'))

    published_date = 'N/A'
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        pub_dt = datetime(*entry.published_parsed[:6])
        published_date = pub_dt.strftime('%Y-%m-%d %H:%M:%S')
    elif 'published' in entry:
        published_date = entry.published

    return {
        'title': title,
        'link': link,
        'summary': summary,
        'published_date': published_date,
        'source': source,
    }


def extract_live(feeds: dict) -> list[dict]:
    """Extract all live articles from RSS feeds."""
    articles = []

    for source, url in feeds.items():
        logger.info('LIVE: Processing: %s', source)
        feed = fetch_feed(url)
        time.sleep(0.2)

        if feed is None:
            continue

        for entry in feed.entries:
            article = extract_entry_fields(entry, source)
            articles.append(article)

    logger.info('LIVE: Extracted %d live articles total', len(articles))
    return articles

