"""Extract live tech news from RSS
feeds."""

import feedparser
import requests
import time
from typing import Optional
from datetime import datetime
from logger import logger
import psycopg2
import pandas as pd
from rss_load import get_connection


RSS_FEEDS = {
    'techcrunch': 'https://techcrunch.com/feed/',
    'hackernews': 'https://hnrss.org/frontpage'
}


def get_latest_article_date() -> Optional[datetime]:
    """Query RDS for the latest article published_date to avoid reprocessing."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(published_date) FROM rss_article")
            result = cur.fetchone()
        conn.close()

        if result and result[0]:
            return result[0]
    except Exception as e:
        logger.warning("Failed to get latest article date from RDS: %s", e)

    return None


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
    url = entry.get('link', 'N/A')
    summary = entry.get('summary', entry.get('description', 'N/A'))

    published_date = 'N/A'
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        pub_dt = datetime(*entry.published_parsed[:6])
        published_date = pub_dt.strftime('%Y-%m-%d %H:%M:%S')
    elif 'published' in entry:
        published_date = entry.published

    return {
        'title': title,
        'url': url,
        'summary': summary,
        'published_date': published_date,
        'source': source,
    }


def extract_live(feeds: dict) -> list[dict]:
    """Extract all live articles from RSS feeds that are newer than the latest in RDS."""
    articles = []

    # Get the latest article date from RDS
    latest_date = get_latest_article_date()
    if latest_date:
        logger.info('LIVE: Only fetching articles after %s', latest_date)

    for source, url in feeds.items():
        logger.info('LIVE: Processing: %s', source)
        feed = fetch_feed(url)
        time.sleep(0.2)

        if feed is None:
            continue

        for entry in feed.entries:
            article = extract_entry_fields(entry, source)

            # Skip articles already present in RDS (full datetime comparison)
            if latest_date and article['published_date'] != 'N/A':
                try:
                    article_dt = datetime.strptime(
                        article['published_date'], '%Y-%m-%d %H:%M:%S')
                    if article_dt <= latest_date:
                        continue
                except Exception as e:
                    logger.warning("Failed to compare dates %s vs %s: %s",
                                   article['published_date'], latest_date, e)

            articles.append(article)

    logger.info('LIVE: Extracted %d new live articles total', len(articles))
    return articles


if __name__ == '__main__':
    articles = extract_live(RSS_FEEDS)
    for art in articles:
        print(f"{art['published_date']} - {art['title']} ({art['url']})")
