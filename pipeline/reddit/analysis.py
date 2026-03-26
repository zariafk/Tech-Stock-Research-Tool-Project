"""OpenAI-powered analysis of Reddit posts for ticker relevance and sentiment."""

import json
import time
import hashlib
import logging

import pandas as pd
from openai import OpenAI


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def format_ticker_prompt(post: dict, tickers: list[str]) -> str:
    """Builds the OpenAI prompt for a single post."""
    return f"""
    Act as: Senior Quant Analyst.
    Universe: {", ".join(tickers)}
    Input: "{post['title']}" | "{post['selftext']}"

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
    [{{"t": "TICKER", "r": [score], "s": [score], "why": "one sentence justification"}}]
    """


def extract_keywords(
    post: dict,
    ticker_companies: dict[str, str],
) -> list[str]:
    """Finds tickers mentioned via keyword match in title and selftext."""
    text = f"{post['title']} {post['selftext']}".lower()
    return [
        ticker for ticker, company in ticker_companies.items()
        if ticker.lower() in text or company.lower() in text
    ]


def parse_relevance_data(response: str) -> list[dict]:
    """Parses OpenAI JSON response into a list of ticker results."""
    try:
        cleaned = (
            response.strip()
            .replace("```json", "")
            .replace("```", "")
            .strip()
        )
        data = json.loads(cleaned)

        if isinstance(data, dict):
            data = [data]

        return [
            {
                "ticker": item.get("t"),
                "relevance_score": item.get("r"),
                "sentiment": item.get("s"),
                "analysis": item.get("why"),
            }
            for item in data
            if item.get("r", 0) >= 7
        ]

    except (json.JSONDecodeError, TypeError) as exc:
        logger.error("Failed to parse OpenAI response: %s", exc)
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

        except Exception as exc:
            if "rate_limit" in str(exc).lower() and attempt < max_retries - 1:
                backoff = 2 ** attempt
                logger.warning(
                    "Rate limited, retrying in %ds (attempt %d/%d)",
                    backoff, attempt + 1, max_retries,
                )
                time.sleep(backoff)
            else:
                logger.error("OpenAI error after %d attempts: %s",
                             attempt + 1, exc)
                return []

    return []


def analyse_posts(
    fact_posts: pd.DataFrame,
    *,
    ticker_companies: dict[str, str],
    client: OpenAI,
    delay: float = 0.2,
) -> pd.DataFrame:
    """Analyses each post for ticker relevance and sentiment.

    Returns a DataFrame with one row per post-ticker pair, suitable
    for loading into a fact_post_tickers table.
    """
    rows = []

    for _, post in fact_posts.iterrows():
        post_dict = post.to_dict()

        # Fast keyword pre-filter
        matched_tickers = extract_keywords(post_dict, ticker_companies)
        if not matched_tickers:
            continue

        # OpenAI analysis on matched tickers only
        results = get_ticker_analysis(client, post_dict, matched_tickers)

        for result in results:
            rows.append({
                "post_id": post_dict["id"],
                "ticker": result["ticker"],
                "relevance_score": result["relevance_score"],
                "sentiment": result["sentiment"],
                "analysis": result["analysis"],
            })

        time.sleep(delay)

    df = pd.DataFrame(rows)

    if df.empty:
        logger.warning("No relevant ticker matches found")
        return pd.DataFrame(columns=[
            "post_id", "ticker", "relevance_score", "sentiment", "analysis",
        ])

    df = df.drop_duplicates(subset=["post_id", "ticker"])
    logger.info("fact_post_tickers: %d rows", len(df))
    return df
