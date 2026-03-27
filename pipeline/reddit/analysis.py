"""OpenAI-powered analysis of Reddit posts for ticker relevance and sentiment."""

import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from openai import OpenAI, RateLimitError, APIError


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_WORKERS = 3


def format_ticker_prompt(post: dict, tickers: list[str]) -> str:
    """Builds the OpenAI prompt for a single post."""
    return f"""
    Act as: Senior Quant Analyst.
    Universe: {", ".join(tickers)}
    Input: "{post['title']}" | "{post['contents']}"

    Task: Score Relevance (0-10) and Sentiment (-1.0 to 1.0).

    Relevance Rubric:
    - 10: Direct idiosyncratic event (Earnings, M&A).
    - 8: Significant business news (New product, contract).
    - 7: Indirect impact (Competitor/Sector news).
    - <7: Ignore.

    Sentiment Rubric (Strictly use these values):
    - 1.0: Transformational positive news.
    - 0.5: Incremental/Standard positive news.
    - 0.0: Neutral/Mixed news.
    - -0.5: Incremental negative news.
    - -1.0: Catastrophic negative news.

    Output Format (JSON list):
    [{{"t": "TICKER", "r": score, "s": score, "why": "one sentence justification"}}]
    """


def extract_keywords(
    post: dict,
    ticker_companies: dict[str, str],
) -> list[str]:
    """Finds tickers mentioned via keyword match in title and contents."""
    text = f"{post['title']} {post['contents']}".lower()
    return [
        ticker for ticker, company in ticker_companies.items()
        if ticker.lower() in text or company.lower() in text
    ]


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
            # Filter for r >= 7
            if item.get('r', 0) >= 7:
                results.append({
                    "ticker": item.get('t'),
                    "relevance_score": item.get('r'),
                    "sentiment": item.get('s'),
                    "analysis": item.get('why')
                })
        return results
    except json.JSONDecodeError as e:
        print(item.get('r', 0))
        print(f"the type is: {item.get('r', 0)}")
        logger.error(
            f"Failed to parse JSON from OpenAI response: {e}. Raw: {response}")
        return []
    except (KeyError, TypeError, AttributeError) as e:
        logger.error(
            f"Unexpected structure in OpenAI response: {e}. Raw: {response}")
        return []


def get_ticker_analysis(
    client: OpenAI,
    post: dict,
    tickers: list[str],
    *,
    model: str = "gpt-4o-mini",
    max_retries: int = 3,
) -> list[dict]:
    """Calls OpenAI for relevance and sentiment scoring with retry logic."""
    prompt = format_ticker_prompt(post, tickers)

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
            )
            return parse_relevance_data(response.choices[0].message.content)

        except RateLimitError as exc:
            if attempt < max_retries - 1:
                backoff = 2 ** attempt
                logger.warning(
                    "Rate limited, retrying in %ds (attempt %d/%d)",
                    backoff, attempt + 1, max_retries,
                )
                time.sleep(backoff)
            else:
                logger.error(
                    "Rate limit persisted after %d attempts: %s",
                    max_retries, exc,
                )
                return []

        except APIError as exc:
            logger.error("OpenAI API error on attempt %d: %s",
                         attempt + 1, exc)
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return []

    return []


def _analyse_single_post(args: tuple) -> list[dict]:
    """Analyses a single post — designed to run inside a thread pool."""
    post_dict, matched_tickers, client = args

    results = get_ticker_analysis(client, post_dict, matched_tickers)

    return [
        {
            "post_id": post_dict["post_id"],
            "ticker": result["ticker"],
            "relevance_score": result["relevance_score"],
            "sentiment": result["sentiment"],
            "analysis": result["analysis"],
        }
        for result in results
    ]


def analyse_posts(
    fact_posts: pd.DataFrame,
    *,
    ticker_companies: dict[str, str],
    client: OpenAI,
    max_workers: int = MAX_WORKERS,
) -> pd.DataFrame:
    """Analyses each post for ticker relevance and sentiment.

    Returns a DataFrame with one row per post-ticker pair, suitable
    for loading into a story_stock table.
    """
    # Pre-filter: only send posts with keyword matches to OpenAI
    posts_to_analyse = []
    for _, post in fact_posts.iterrows():
        post_dict = post.to_dict()
        matched_tickers = extract_keywords(post_dict, ticker_companies)
        if matched_tickers:
            posts_to_analyse.append((post_dict, matched_tickers, client))

    logger.info(
        "Analysing %d/%d posts with OpenAI (%d workers)",
        len(posts_to_analyse), len(fact_posts), max_workers,
    )

    # Parallel OpenAI calls
    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        batches = list(executor.map(_analyse_single_post, posts_to_analyse))

    for batch in batches:
        rows.extend(batch)

    df = pd.DataFrame(rows)

    if df.empty:
        logger.warning("No relevant ticker matches found")
        return pd.DataFrame(columns=[
            "post_id", "ticker", "relevance_score", "sentiment", "analysis",
        ])

    df = df.drop_duplicates(subset=["post_id", "ticker"])
    logger.info("fact_post_tickers: %d rows", len(df))
    return df
