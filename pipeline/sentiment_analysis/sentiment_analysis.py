"""Sentiment analysis using OpenAI chat completions API."""

import os
from dotenv import load_dotenv
from logger import logger
from openai import OpenAI, OpenAIError

load_dotenv()

# this should be in same directory as logger.py, so it can be imported directly

MODEL = "gpt-3.5-turbo"
TEMPERATURE = 0
POSITIVE_THRESHOLD = 0.25
NEGATIVE_THRESHOLD = -0.25
SYSTEM_PROMPT = (
    "You are a precise sentiment analysis tool. "
    "Return a single float between -1.0 (very negative) and 1.0 (very positive). "
    "Return ONLY the number. No words, no punctuation."
)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def validate_text(text: str) -> bool:
    """Return False if text is empty or whitespace-only."""
    return bool(text.strip())


def call_sentiment_api(text: str) -> str:
    """Call OpenAI API and return the raw response string."""
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
        temperature=TEMPERATURE,
    )

    ai_response = response.choices[0].message.content.strip()

    return ai_response


def parse_sentiment_score(raw: str) -> float:
    """Parse raw API string to float. Raises ValueError if not numeric."""
    return float(raw)


def analyze_sentiment(text: str) -> float:
    """Return a sentiment score from -1.0 to 1.0; 0.0 for empty input."""
    if not validate_text(text):
        return 0.0
    try:
        raw = call_sentiment_api(text)
    except OpenAIError as e:
        logger.error("API error: %s", e)
        return 0.0
    try:
        return parse_sentiment_score(raw)
    except ValueError as e:
        logger.error("Parse error: %s", e)
        return 0.0
