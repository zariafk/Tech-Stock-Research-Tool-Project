"""
EXTRACT: Historical Tech Sentiment Ingestion
Source: Hacker News (via Algolia API)
Target Persona: Long-Term Hedge Fund Analyst
Goal: Capture 'Developer Mindshare' as a leading indicator for tech equities.
"""

import requests
import time
from datetime import datetime, timedelta
from logger import logger

# Constants for API configuration and lookback window
ALGOLIA_HN_URL = 'https://hn.algolia.com/api/v1/search'
# HN_HISTORY_YEARS = 2  # Standard lookback for identifying long-term tech shifts
HN_MAX_RESULTS = 10   # Filter for top-relevance hits to manage LLM token costs


def get_hn_historical(company_name: str) -> list[dict]:
    """
    Fetches high-engagement 'Stories' from Hacker News within a specific lookback period.
    Uses 'Points' as a proxy for community validation/impact.
    """
    # Define start of lookback period (fixed to January 1, 2024)
    past_date = datetime(2024, 1, 1)
    timestamp = int(past_date.timestamp())

    # Filter for 'story' tags only and use company name as the query for better relevance on HN
    params = {
        'query': company_name,
        'tags': 'story',
        'numericFilters': f'created_at_i>{timestamp}'
    }

    try:
        response = requests.get(ALGOLIA_HN_URL, params=params, timeout=10)

        # Guard clause for API failures (critical for automated pipelines)
        if response.status_code != 200:
            logger.error('HIST: Algolia API Failure | %s | Code: %s',
                         company_name, response.status_code)
            return []

        data = response.json()
        articles = []

        # Parse results into a standardized schema for downstream RAG consumption
        for hit in data.get('hits', [])[:HN_MAX_RESULTS]:
            # Use title as summary (HN titles are designed to be concise article descriptions)
            # Add community engagement signals to approximate article significance
            title = hit.get('title', 'N/A')
            points = hit.get('points', 0)
            comments = hit.get('num_comments', 0)
            summary = f"{title} (Engagement: {points} points, {comments} comments)"

            article = {
                'title': title,
                # Try 'url' first, then 'story_url' (algonlia sometimes nests URLs differently)
                'url': hit.get('url', hit.get('story_url', 'N/A')),
                # Full summary with engagement context (matches TechCrunch RSS style more closely)
                'summary': summary,
                'published_date': datetime.fromtimestamp(hit['created_at_i']).strftime('%Y-%m-%d %H:%M:%S'),
                'source': 'algolia_hn',
            }
            articles.append(article)

        logger.info(
            'HIST: Successfully fetched %d signals for %s',
            len(articles), company_name
        )
        return articles

    except requests.exceptions.RequestException as e:
        logger.error(
            'HIST: Network error during extraction for %s: %s',
            company_name, e
        )
        return []


def extract_historical(tickers_map: dict) -> list[dict]:
    """
    Orchestrates the extraction across the entire watchlist.
    Maps company-level sentiment back to specific stock tickers.
    """
    articles = []

    for ticker, company in tickers_map.items():
        # Fetching data by Company Name (better for HN) but tagging by Ticker (better for Finance)
        hn_articles = get_hn_historical(company)

        for article in hn_articles:
            # Create a hard copy to avoid reference issues during multi-ticker mapping
            article_copy = article.copy()
            article_copy['ticker'] = ticker
            articles.append(article_copy)

        # Politeness delay to avoid IP blacklisting (Rate Limit Management)
        time.sleep(0.1)

    logger.info(
        'HIST: Batch Extraction Complete: %d historical signals mapped to %d tickers.',
        len(articles), len(tickers_map)
    )
    return articles
