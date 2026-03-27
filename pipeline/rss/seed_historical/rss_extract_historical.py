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
from concurrent.futures import ThreadPoolExecutor

# Constants for API configuration and lookback window
ALGOLIA_HN_URL = 'https://hn.algolia.com/api/v1/search'
# HN_HISTORY_YEARS = 2  # Standard lookback for identifying long-term tech shifts
HN_MAX_RESULTS = 10   # Filter for top-relevance hits to manage LLM token costs
MAX_WORKERS = 5       # Concurrent threads for parallel extraction


def get_hn_historical(company_name: str) -> list[dict]:
    """
    Fetches high-engagement 'Stories' from Hacker News within a specific lookback period.
    Uses 'Points' as a proxy for community validation/impact.
    """
    # Fixed lookback: Jan 1, 2024 onward
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

        # Return empty set on API failure; prevents partial ingestion
        if response.status_code != 200:
            logger.error('HIST: Algolia API Failure | %s | Code: %s',
                         company_name, response.status_code)
            return []

        data = response.json()
        articles = []

        # Limit to top HN_MAX_RESULTS to control LLM token costs
        for hit in data.get('hits', [])[:HN_MAX_RESULTS]:
            title = hit.get('title', 'N/A')
            points = hit.get('points', 0)
            comments = hit.get('num_comments', 0)
            # Engagement metrics aid SAR signal interpretation
            summary = f"{title} (Engagement: {points} points, {comments} comments)"

            article = {
                'title': title,
                # Algolia nests URLs; fallback preserves compatibility
                'url': hit.get('url', hit.get('story_url', 'N/A')),
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


def _extract_for_ticker(ticker_company_pair: tuple) -> list[dict]:
    """Extract and tag articles for a single ticker from Hacker News."""
    ticker, company = ticker_company_pair
    articles = get_hn_historical(company)

    result = []
    for article in articles:
        # Deep copy prevents cross-ticker reference contamination in concurrent context
        article_copy = article.copy()
        article_copy['ticker'] = ticker
        result.append(article_copy)

    return result


def extract_historical(tickers_map: dict) -> list[dict]:
    """Orchestrates parallel extraction across the watchlist using ThreadPoolExecutor."""
    # Concurrent extraction: each ticker fetched in parallel thread
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(_extract_for_ticker, tickers_map.items()))

    # Flatten list of lists from concurrent results
    articles = []
    for batch in results:
        articles.extend(batch)

    logger.info(
        'HIST: Batch Extraction Complete: %d historical signals mapped to %d tickers.',
        len(articles), len(tickers_map)
    )
    return articles
