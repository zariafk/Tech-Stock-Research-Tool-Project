"""OpenAI-powered analysis of RSS articles for ticker relevance and sentiment."""

import boto3
from openai import OpenAI, RateLimitError, APIError
from concurrent.futures import ThreadPoolExecutor
import os
import dotenv
import time
import pandas as pd
import json
import hashlib
import psycopg2
import re
from logger import logger
from fallback_stock import tech_universe
from rss_extract_live import RSS_FEEDS, extract_live
from rss_load import get_connection, get_tickers_from_db

dotenv.load_dotenv()


def get_existing_urls(urls: list[str]) -> set[str]:
    """Batch-check which URLs from the current run already exist in rss_article.

    Called fresh per pipeline run so it reflects the current DB state,
    unlike a module-level cache which goes stale between runs.
    """
    if not urls:
        return set()
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT url FROM rss_article WHERE url = ANY(%s)",
                (list(urls),)
            )
            results = cur.fetchall()
        conn.close()
        return {row[0] for row in results}
    except psycopg2.OperationalError as e:
        logger.error("Database connection error checking existing URLs: %s", e)
        return set()
    except psycopg2.Error as e:
        logger.error("Database error checking existing URLs: %s", e)
        return set()


def get_ticker_companies_from_db() -> dict:
    """Get ticker -> stock_name mapping from the stock table."""
    try:
        conn = get_connection()
        stock_lookup = get_tickers_from_db(conn)
        conn.close()
        # Convert { ticker: {"stock_id": ..., "stock_name": ...} } to { ticker: "stock_name" }
        return {ticker: info["stock_name"] for ticker, info in stock_lookup.items()}
    except psycopg2.OperationalError as e:
        logger.error(
            "Database connection error fetching tickers, using default tech_universe: %s", e)
        return tech_universe
    except (psycopg2.Error, KeyError, AttributeError) as e:
        logger.error(
            "Error processing tickers from DB, using default tech_universe: %s", e)
        return tech_universe


TICKER_COMPANIES = get_ticker_companies_from_db()
TECH_TICKERS = list(TICKER_COMPANIES.keys())
MAX_WORKERS = 20  # Concurrent OpenAI threads to respect rate limits


def get_secret(secret_name: str, region: str = "eu-west-2") -> dict:
    """Retrieves a secret from AWS Secrets Manager and returns it as a dict."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_openai_client() -> OpenAI:
    """Initialize OpenAI client using API key from AWS Secrets Manager."""
    try:
        secret = get_secret("c22-trade-research-tool-secrets")
        api_key = secret.get('api_key') or secret.get('OPENAI_API_KEY')
        return OpenAI(api_key=api_key)
    except Exception as e:
        logger.error(
            "Failed to retrieve OpenAI API key from Secrets Manager: %s", e)
        raise


CLIENT = get_openai_client()


def format_ticker_prompt(entry: dict, tickers: list[str]) -> str:
    return f"""
    Act as: Senior Quant Analyst.
    Universe: {", ".join(tickers)}
    Input: "{entry['title']}" | "{entry['summary']}"

    Task: Score Relevance (0-10), Sentiment (-1.0 to 1.0), and Confidence in your scoring.

    Relevance Rubric:
    - 10: Direct idiosyncratic event (Earnings, M&A).
    - 8: Significant business news (New product, contract).
    - 7: Indirect impact (Competitor/Sector news).
    - <7: Ignore.

    Sentiment Rubric (Score from -1.0 to 1.0 in 0.1 increments):
    - +1.0: Transformational/Systemic positive (M&A, massive earnings beat).
    - +0.7 to +0.9: Strong positive (Major product launch, key contract win).
    - +0.3 to +0.6: Moderate positive (Positive analyst upgrade, steady growth).
    - +0.1 to +0.2: Slight positive (Minor positive mention, general market lift).
    - 0.0: Neutral, administrative, or purely factual noise.
    - -0.1 to -0.2: Slight negative (Minor litigation, general market drag).
    - -0.3 to -0.6: Moderate negative (Missed estimates, management turnover).
    - -0.7 to -0.9: Strong negative (Regulatory investigation, product recall).
    - -1.0: Catastrophic (Bankruptcy, fraud, massive data breach).

    Confidence Scoring:
    - High: Clear signal. Direct quotes, confirmed by multiple sources, or explicit event.
    - Medium: Reasonable inference but some ambiguity. Analyst opinion or sector trend.
    - Low: Speculative or based on rumor. Requires corroboration.
    - Unknown: Insufficient information to determine confidence.

    Output Format (JSON list):
    [{{
      "t": "TICKER",
      "r": score,
      "s": score,
      "c": "High|Medium|Low",
      "why": "one sentence justification"
    }}]
    """


def extract_keywords(entry: dict, tickers: list[str]) -> list[str]:
    """Find tickers mentioned via keyword match."""
    text = f"{entry['title']} {entry['summary']}".lower()
    matches = []

    for ticker in tickers:
        if ticker not in TICKER_COMPANIES:
            continue
        company = TICKER_COMPANIES[ticker].lower()
        t_low = ticker.lower()
        # if (ticker.lower() in text) or (company in text):
        #     matches.append(ticker)
        if re.search(rf'\b{t_low}\b', text) or (company in text):
            matches.append(ticker)
    logger.info("%s articles matched tickers via keyword: %s, company: %s",
                len(matches), matches, company)
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
            # DEBUG: Log all OpenAI scores before filtering
            logger.debug(
                f"OpenAI scored ticker {item.get('t')} with relevance {item.get('r')} (why: {item.get('why')})")

            # Filter for r >= 6 and only include tickers in our database
            if item.get('r', 0) >= 6 and item.get('t') in TICKER_COMPANIES:
                results.append({
                    "ticker": item.get('t'),
                    "relevance_score": item.get('r'),
                    "sentiment": item.get('s'),
                    "confidence": item.get('c', 'Unknown'),
                    "analysis": item.get('why')
                })
            elif item.get('t') in TICKER_COMPANIES:
                logger.info(
                    f"Filtered out {item.get('t')}: relevance {item.get('r')} < 6")
        return results
    except json.JSONDecodeError as e:
        logger.error(
            f"Failed to parse JSON from OpenAI response: {e}. Raw: {response}")
        return []
    except (KeyError, TypeError, AttributeError) as e:
        logger.error(
            f"Unexpected structure in OpenAI response: {e}. Raw: {response}")
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
        except RateLimitError as e:
            if attempt < max_retries - 1:
                backoff = 2 ** attempt
                logger.warning(
                    f'OpenAI rate limited. Retrying in {backoff}s... (attempt {attempt + 1}/{max_retries})')
                time.sleep(backoff)
            else:
                logger.error(
                    f'OpenAI rate limit persisted after {max_retries} attempts: {e}')
                return []
        except APIError as e:
            logger.error(f'OpenAI API error on attempt {attempt + 1}: {e}')
            if attempt < max_retries - 1:
                backoff = 2 ** attempt
                logger.info(f'Retrying in {backoff}s...')
                time.sleep(backoff)
            else:
                return []


def _analyze_article(article_with_tickers: tuple) -> list[dict]:
    """Analyze single article with OpenAI and enrich results."""
    article, potential_tickers = article_with_tickers
    # Make OpenAI call with retry logic
    analysis = get_ticker_analysis(article, potential_tickers)

    result = []
    for analysis_result in analysis:
        copy = article.copy()
        copy.update(analysis_result)
        result.append(copy)

    return result


def filter_by_ticker(articles: list[dict], tickers: list[str]) -> list[dict]:
    """Filter articles using keyword pre-filter then parallel OpenAI analysis."""
    filtered = []

    logger.info("Starting filter_by_ticker with %d articles", len(articles))

    # Step 0: Single batch DB check — skip any URL that already exists in RDS
    candidate_urls = [a['url'] for a in articles if a['url'] != 'N/A']
    existing_urls = get_existing_urls(candidate_urls)
    logger.info("Found %d/%d articles already in RDS — skipping OpenAI for those.",
                len(existing_urls), len(candidate_urls))

    # Step 1: Pre-filter with keyword matching
    articles_to_analyze = []
    for article in articles:
        if article['url'] in existing_urls:
            logger.debug("Skipping existing article: %s", article['url'])
            continue

        potential_tickers = extract_keywords(article, tickers)
        if potential_tickers:
            articles_to_analyze.append((article, potential_tickers))

    # Step 2: Parallel OpenAI analysis using ThreadPoolExecutor
    logger.info("Analyzing %d articles with OpenAI in parallel",
                len(articles_to_analyze))
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(_analyze_article, articles_to_analyze))

    # Step 3: Flatten results from concurrent execution
    for batch in results:
        filtered.extend(batch)

    return filtered


def generate_article_id(url: str) -> str:
    """Generate a consistent MD5 hash ID from a URL."""
    return hashlib.md5(str(url).encode()).hexdigest()


def create_dataframe(articles: list) -> pd.DataFrame:
    """Create DataFrame and add persistent unique IDs for deduplication."""
    if not articles:
        logger.warning('No articles extracted')
        return pd.DataFrame()

    df = pd.DataFrame(articles)

    df['article_id'] = df['url'].apply(generate_article_id)

    columns = [
        'ticker', 'article_id', 'title', 'url',
        'summary', 'published_date', 'source',
        'relevance_score', 'sentiment', 'confidence', 'analysis'
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
    """Remove duplicate urls before OpenAI processing."""
    seen = set()
    unique = []
    for art in articles:
        if art['url'] not in seen:
            unique.append(art)
            seen.add(art['url'])
    return unique


def analysis(articles: list[dict], tickers: list[str] = None) -> pd.DataFrame:
    """Extract live and historical articles.

    articles can be extract_live(RSS_FEEDS)
    OR extract_historical(TICKER_COMPANIES)
    """
    if tickers is None:
        tickers = TECH_TICKERS

    logger.info('Starting extraction for %d tickers.', len(tickers))

    # Deduplicate BEFORE expensive OpenAI filtering
    raw_articles = deduplicate_raw(articles)
    logger.info('Combined into %d unique articles.', len(raw_articles))

    # Filter by ticker
    filtered = filter_by_ticker(raw_articles, tickers)

    logger.info('Total relevant articles: %d', len(filtered))
    return create_dataframe(filtered)


if __name__ == '__main__':
    # Test confidence scoring on a few sample articles
    test_articles = [
        {
            'title': 'Apple Reports Record Q4 Earnings Beat',
            'summary': 'Apple beat earnings estimates by 15%, guidance strong',
            'url': 'test_1',
            'source': 'Bloomberg'
        },
        {
            'title': 'Apple stock drops on mixed analyst commentary',
            'summary': 'Some analysts downgrade, others maintain hold ratings',
            'url': 'test_2',
            'source': 'Reuters'
        },
        {
            'title': 'Apple might launch new feature next year maybe',
            'summary': 'Rumors suggest Apple could possibly announce something',
            'url': 'test_3',
            'source': 'MobileTech Blog'
        },
        {
            'title': 'Sources: Apple developing new A-series processor architecture',
            'summary': 'People familiar with Apple\'s plans suggest chip redesign underway',
            'url': 'test_low',
            'source': 'Tech Analyst Blog'
        }
    ]

    print("Testing confidence scoring...\n")
    for article in test_articles:
        print(f"Testing: {article['title']}")
        result = get_ticker_analysis(article, ['AAPL'])
        if result:
            for item in result:
                print(f"  Sentiment: {item['sentiment']:.2f}")
                print(f"  Confidence: {item['confidence']}")
                print(f"  Analysis: {item['analysis']}")
        else:
            print("  No results")
        print()
