"""Transform script for RSS pipeline.

Cleans and standardises the raw DataFrame produced by rss_extract.
Output columns: ticker, title, link, summary, published_date, source.
"""

# from pipeline.logger import make_logger

import sys
from pathlib import Path
import pandas as pd


REQUIRED_COLUMNS = [
    "ticker",
    "title",
    "link",
    "summary",
    "published_date",
    "source",
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
    """Remove duplicate rows based on ticker and link."""
    before = len(df)
    df = df.drop_duplicates(subset=["ticker", "link"])
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
    df = df[REQUIRED_COLUMNS]
    logger.info("Transform complete. %d rows remaining.", len(df))
    return df
