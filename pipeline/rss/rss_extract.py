"""Extract orchestrator combining
live and historical sources."""

from rss_extract_historical import (
    extract_historical
)
from rss_extract_live import (
    extract_live,
    RSS_FEEDS
)

from openai import OpenAI
import os
import dotenv
import time
import pandas as pd
import json
import hashlib
from logger import logger

from top_100_tech_companies import tech_universe

dotenv.load_dotenv()

TICKER_COMPANIES = tech_universe
TECH_TICKERS = list(tech_universe.keys())

CLIENT = OpenAI(
    api_key=os.environ.get(
        'OPENAI_API_KEY'
    )
)


def format_ticker_prompt(entry: dict, tickers: list[str]) -> str:
    """Format OpenAI prompt for relevance score into JSON."""
    return f"""
    Act as: Quant Research Assistant.
    Universe: {", ".join(tickers)}
    Input: "{entry['title']}" | "{entry['summary']}"
    
    Task: Extract ticker-specific signals.
    Metrics: 
    - Relevance (0-10, only return if 7+)
    - Sentiment (-1.0 to 1.0, where 0 is neutral)
    
    Output: JSON list only. 
    Format: [{{"t": "TICKER", "r": 9, "s": 0.85, "why": "one sentence"}}]
    """


def extract_keywords(entry: dict, tickers: list[str]) -> list[str]:
    """Find tickers mentioned via keyword match."""
    text = f"{entry['title']} {entry['summary']}".lower()
    matches = []

    for ticker in tickers:
        company = TICKER_COMPANIES[ticker].lower()
        if (ticker.lower() in text) or (company in text):
            matches.append(ticker)

    return matches


def parse_relevance_data(response: str) -> list[dict]:
    """Parse OpenAI JSON response into a list of ticker analytics."""
    try:
        # Strip markdown markers if present
        cleaned = (
            response.strip()
            .replace('```json', '')
            .replace('```', '')
            .strip()
        )
        data = json.loads(cleaned)

        # Ensure it's a list
        if isinstance(data, dict):
            data = [data]

        results = []
        for item in data:
            # We filter for r >= 7 as per yours/user instructions
            if item.get('r', 0) >= 7:
                results.append({
                    "ticker": item.get('t'),
                    "score": item.get('r'),
                    "sentiment": item.get('s'),
                    "analysis": item.get('why')
                })
        return results
    except Exception as e:
        logger.error(f"Failed to parse OpenAI response: {e}. Raw: {response}")
        return []


def get_ticker_analysis(entry: dict, tickers: list[str], max_retries: int = 3) -> list[dict]:
    """Analyze high-potential articles with OpenAI. 
    Retries with exponential backoff on rate limits."""
    prompt = format_ticker_prompt(entry, tickers)

    for attempt in range(max_retries):
        try:
            response = CLIENT.chat.completions.create(
                model='gpt-4o-mini',
                messages=[{'role': 'user', 'content': prompt}]
            )
            return parse_relevance_data(response.choices[0].message.content)
        except Exception as e:
            if 'rate_limit' in str(e).lower() and attempt < max_retries - 1:
                backoff = 2 ** attempt
                logger.warning(
                    f'Rate limited. Retrying in {backoff}s... (attempt {attempt + 1}/{max_retries})')
                time.sleep(backoff)
            else:
                logger.error(f'OpenAI error after {attempt + 1} attempts: {e}')
                return []


def filter_by_ticker(articles: list[dict], tickers: list[str]) -> list[dict]:
    """Filter articles using keyword pre-filter then OpenAI."""
    filtered = []

    for article in articles:
        # Step 1: Fast keyword match (skip if no tickers found)
        potential_tickers = extract_keywords(article, tickers)
        if not potential_tickers:
            continue

        # Step 2: Call OpenAI for relevance/sentiment (with retry backoff)
        analysis = get_ticker_analysis(article, potential_tickers)
        time.sleep(0.2)  # Rate limit protection between calls

        # Step 3: Merge OpenAI results with article, one row per ticker hit
        for result in analysis:
            copy = article.copy()
            copy.update(result)  # Add score, sentiment, analysis
            filtered.append(copy)

    return filtered


def create_dataframe(articles: list) -> pd.DataFrame:
    """Create DataFrame and add persistent unique IDs for deduplication."""
    if not articles:
        logger.warning('No articles extracted')
        return pd.DataFrame()

    df = pd.DataFrame(articles)

    df['article_id'] = df['link'].apply(
        lambda x: hashlib.md5(str(x).encode()).hexdigest())

    columns = [
        'ticker', 'article_id', 'title', 'link',
        'summary', 'published_date', 'source',
        'score', 'sentiment', 'analysis'
    ]

    # Ensure all columns exist (even if some rows didn't get them)
    for col in columns:
        if col not in df.columns:
            df[col] = None

    df = df[columns]

    df['published_date'] = (
        pd.to_datetime(
            df['published_date'],
            errors='coerce'
        )
    )

    df = df.sort_values(
        by=[
            'ticker',
            'published_date'
        ],
        ascending=[True, False]
    ).reset_index(drop=True)

    logger.info(
        'DataFrame shape: %s',
        df.shape
    )
    return df


def deduplicate_raw(articles: list[dict]) -> list[dict]:
    """Remove duplicate links before OpenAI processing."""
    seen = set()
    unique = []
    for art in articles:
        if art['link'] not in seen:
            unique.append(art)
            seen.add(art['link'])
    return unique


def extract(tickers: list[str] = None) -> pd.DataFrame:
    """Extract live and historical articles."""
    if tickers is None:
        tickers = TECH_TICKERS

    logger.info('Starting extraction for %d tickers.', len(tickers))

    # Fetch sources
    live = extract_live(RSS_FEEDS)
    historical = extract_historical(TICKER_COMPANIES)

    # Deduplicate BEFORE expensive OpenAI filtering
    raw_articles = deduplicate_raw(live + historical)
    logger.info('Combined into %d unique articles.', len(raw_articles))

    # Filter by ticker
    filtered = filter_by_ticker(raw_articles, tickers)

    logger.info('Total relevant articles: %d', len(filtered))
    return create_dataframe(filtered)


if __name__ == '__main__':
    # Run a test extraction
    df = extract()

    # Store locally for testing as requested
    if not df.empty:
        filename = f"test_results_{int(time.time())}.csv"
        df.to_csv(filename, index=False)
        logger.info(f"Results stored for testing in {filename}")
        print(df.head())
    else:
        logger.warning("No data extracted to store.")
