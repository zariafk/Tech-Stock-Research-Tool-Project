"""Unit tests for the RSS transform module."""

import pandas as pd
import pytest
from rss_transform import (
    validate_dataframe,
    drop_incomplete_rows,
    strip_whitespace,
    normalise_published_date,
    deduplicate,
    transform,
)


# === Fixtures ===

@pytest.fixture
def valid_df():
    """Minimal valid DataFrame matching the extract output schema."""
    return pd.DataFrame([
        {
            "ticker": "AAPL",
            "title": "Apple hits record",
            "link": "https://example.com/apple",
            "summary": "Apple shares rose today.",
            "published_date": "2026-03-24 10:00:00",
            "source": "Yahoo Finance",
        },
        {
            "ticker": "MSFT",
            "title": "Microsoft acquires startup",
            "link": "https://example.com/msft",
            "summary": "Microsoft announced an acquisition.",
            "published_date": "2026-03-23 09:00:00",
            "source": "Reuters",
        },
    ])


# === validate_dataframe ===

def test_validate_dataframe_passes_on_valid(valid_df):
    validate_dataframe(valid_df)


def test_validate_dataframe_raises_on_missing_column(valid_df):
    df = valid_df.drop(columns=["ticker"])
    with pytest.raises(ValueError, match="Missing column: ticker"):
        validate_dataframe(df)


# === drop_incomplete_rows ===

def test_drop_incomplete_rows_removes_na(valid_df):
    valid_df.loc[0, "title"] = None
    result = drop_incomplete_rows(valid_df)
    assert len(result) == 1
    assert result.iloc[0]["ticker"] == "MSFT"


def test_drop_incomplete_rows_removes_na_string(valid_df):
    valid_df.loc[1, "link"] = "N/A"
    result = drop_incomplete_rows(valid_df)
    assert len(result) == 1
    assert result.iloc[0]["ticker"] == "AAPL"


def test_drop_incomplete_rows_keeps_complete(valid_df):
    result = drop_incomplete_rows(valid_df)
    assert len(result) == 2


# === strip_whitespace ===

def test_strip_whitespace_removes_padding(valid_df):
    valid_df.loc[0, "title"] = "  Apple hits record  "
    result = strip_whitespace(valid_df)
    assert result.loc[0, "title"] == "Apple hits record"


# === normalise_published_date ===

def test_normalise_published_date_converts_strings(valid_df):
    result = normalise_published_date(valid_df)
    assert pd.api.types.is_datetime64_any_dtype(result["published_date"])


def test_normalise_published_date_drops_unparseable(valid_df):
    valid_df.loc[0, "published_date"] = "not-a-date"
    result = normalise_published_date(valid_df)
    assert len(result) == 1
    assert result.iloc[0]["ticker"] == "MSFT"


# === deduplicate ===

def test_deduplicate_removes_duplicate_ticker_link(valid_df):
    duplicate = valid_df.iloc[[0]].copy()
    df = pd.concat([valid_df, duplicate], ignore_index=True)
    result = deduplicate(df)
    assert len(result) == 2


def test_deduplicate_keeps_different_tickers_same_link():
    df = pd.DataFrame([
        {"ticker": "AAPL", "link": "https://example.com/news"},
        {"ticker": "MSFT", "link": "https://example.com/news"},
    ])
    result = deduplicate(df)
    assert len(result) == 2


# === transform (integration) ===

def test_transform_returns_dataframe(valid_df):
    result = transform(valid_df)
    assert isinstance(result, pd.DataFrame)


def test_transform_output_columns(valid_df):
    result = transform(valid_df)
    expected = ["ticker", "title", "link",
                "summary", "published_date", "source"]
    assert list(result.columns) == expected


def test_transform_raises_on_missing_column(valid_df):
    df = valid_df.drop(columns=["source"])
    with pytest.raises(ValueError):
        transform(df)


def test_transform_drops_na_rows(valid_df):
    valid_df.loc[0, "published_date"] = "N/A"
    result = transform(valid_df)
    assert len(result) == 1


def test_transform_empty_after_cleaning():
    df = pd.DataFrame([{
        "ticker": "AAPL",
        "title": "N/A",
        "link": "N/A",
        "summary": "N/A",
        "published_date": "N/A",
        "source": "Yahoo Finance",
    }])
    result = transform(df)
    assert result.empty
