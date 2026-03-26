"""Transform script for RSS pipeline.

Cleans and standardises the enriched DataFrame produced by rss_extract.
Output columns: ticker, article_id, title, link, summary, published_date, source, score, sentiment, analysis.
Deduplication across S3 runs is handled in rss_load.py via article_id.
"""

# from pipeline.logger import make_logger

import sys
from pathlib import Path
import pandas as pd
from logger import logger


REQUIRED_COLUMNS = [
    "ticker",
    "article_id",
    "title",
    "link",
    "summary",
    "published_date",
    "source",
    "score",
    "sentiment",
    "analysis",
]


def validate_dataframe(df: pd.DataFrame) -> None:
    """Raise ValueError if df is missing any required columns."""
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")


def drop_incomplete_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where title, link, or published_date are missing."""
    required = ["title", "link", "published_date"]
    before = len(df)
    df = df.dropna(subset=required)
    df = df[~df[required].isin(["N/A", ""]).any(axis=1)]
    logger.info("Dropped %d incomplete rows.", before - len(df))
    return df.reset_index(drop=True)


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    string_cols = df.select_dtypes(include="object").columns
    df[string_cols] = df[string_cols].apply(lambda col: col.str.strip())
    return df


def normalise_published_date(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce published_date to datetime; drop rows that cannot be parsed."""
    before = len(df)
    df["published_date"] = pd.to_datetime(
        df["published_date"], errors="coerce", utc=True
    )
    df = df.dropna(subset=["published_date"])
    logger.info("Dropped %d rows with unparseable dates.", before - len(df))
    return df.reset_index(drop=True)


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows based on article_id (within this run).
    Cross-run deduplication against S3 is handled in rss_load.py.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["article_id"])
    logger.info("Dropped %d duplicate rows.", before - len(df))
    return df.reset_index(drop=True)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Run all cleaning steps and return a standardised DataFrame."""
    validate_dataframe(df)
    logger.info("Starting transform on %d rows.", len(df))
    df = strip_whitespace(df)
    df = drop_incomplete_rows(df)
    df = normalise_published_date(df)
    df = deduplicate(df)
    prepare_for_rag(df)
    df = df[REQUIRED_COLUMNS]
    logger.info("Transform complete. %d rows remaining.", len(df))
    return df


def prepare_for_rag(df: pd.DataFrame) -> list[dict]:
    """Convert transformed DataFrame into RAG-ready documents.

    Each document contains:
      - id: unique article identifier (article_id hash)
      - text: content to be embedded (title + summary + AI analysis)
      - metadata: structured fields for filtering at retrieval time
    """
    documents = []

    for _, row in df.iterrows():
        # Combine fields into a single embeddable text block
        text = f"Title: {row['title']}\nSummary: {row['summary']}"
        if row.get('analysis'):
            text += f"\nAnalysis: {row['analysis']}"

        doc = {
            "id": row["article_id"],
            "text": text,
            "metadata": {
                "ticker": row["ticker"],
                "source": row["source"],
                "published_date": str(row["published_date"]),
                "link": row["link"],
                "relevance_score": row.get("score"),
                "sentiment": row.get("sentiment"),
            }
        }
        documents.append(doc)

    logger.info("Prepared %d RAG documents.", len(documents))
    return documents
