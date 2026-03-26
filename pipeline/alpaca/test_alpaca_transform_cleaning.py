"""Tests for the alpaca_transform_cleaning module."""

import pandas as pd
import pytest

from alpaca_transform_cleaning import (
    convert_datetime_columns,
    convert_numeric_columns,
    remove_duplicates,
    ensure_required_columns_exist,
    validate_symbol,
    validate_number,
    validate_bar_price_relationships,
    validate_stock_bar_row,
    validate_stock_snapshot_row,
    transform_stock_bars,
)


@pytest.fixture
def valid_symbols():
    """Provide a small stock universe used across validation tests."""
    return ["AAPL", "MSFT", "NVDA"]


@pytest.fixture
def valid_bar_row():
    """Provide one valid stock bar row for row-level validation tests."""
    return pd.Series({
        "symbol": "AAPL",
        "bar_timestamp": pd.Timestamp("2026-03-25T04:00:00Z"),
        "bar_date": pd.Timestamp("2026-03-25T04:00:00Z"),
        "open": 100.0,
        "high": 110.0,
        "low": 95.0,
        "close": 105.0,
        "volume": 1000,
        "trade_count": 25,
        "vwap": 103.0,
        "ingestion_time": pd.Timestamp("2026-03-25T22:21:50Z"),
    })


@pytest.fixture
def valid_snapshot_row():
    """Provide one valid stock snapshot row for row-level validation tests."""
    return pd.Series({
        "symbol": "MSFT",
        "snapshot_time": pd.Timestamp("2026-03-25T19:59:54Z"),
        "latest_trade_price": 300.0,
        "previous_close": 295.0,
        "current_day_open": 297.0,
        "current_day_high": 305.0,
        "current_day_low": 294.0,
        "current_day_volume": 50000,
        "current_day_vwap": 299.5,
        "current_day_trade_count": 1200,
        "ingestion_time": pd.Timestamp("2026-03-25T22:21:51Z"),
    })


@pytest.fixture
def bars_dataframe():
    """Provide a small bars dataframe with one duplicate row."""
    return pd.DataFrame([
        {
            "symbol": "AAPL",
            "bar_timestamp": "2026-03-25T04:00:00Z",
            "bar_date": "2026-03-25T04:00:00Z",
            "open": "100.0",
            "high": "110.0",
            "low": "95.0",
            "close": "105.0",
            "volume": "1000",
            "trade_count": "25",
            "vwap": "103.0",
            "ingestion_time": "2026-03-25T22:21:50Z",
        },
        {
            "symbol": "AAPL",
            "bar_timestamp": "2026-03-25T04:00:00Z",
            "bar_date": "2026-03-25T04:00:00Z",
            "open": "100.0",
            "high": "110.0",
            "low": "95.0",
            "close": "105.0",
            "volume": "1000",
            "trade_count": "25",
            "vwap": "103.0",
            "ingestion_time": "2026-03-25T22:21:50Z",
        },
        {
            "symbol": "MSFT",
            "bar_timestamp": "2026-03-25T04:00:00Z",
            "bar_date": "2026-03-25T04:00:00Z",
            "open": "200.0",
            "high": "210.0",
            "low": "190.0",
            "close": "205.0",
            "volume": "1500",
            "trade_count": "30",
            "vwap": "202.0",
            "ingestion_time": "2026-03-25T22:21:50Z",
        },
    ])


def test_convert_datetime_columns_parses_valid_and_invalid_values():
    """Convert valid datetime strings and coerce invalid ones to NaT."""
    df = pd.DataFrame({
        "bar_timestamp": ["2026-03-25T04:00:00Z", "bad_value"]
    })

    result = convert_datetime_columns(df, ["bar_timestamp"])

    assert pd.notna(result.loc[0, "bar_timestamp"])
    assert pd.isna(result.loc[1, "bar_timestamp"])


def test_convert_numeric_columns_parses_numbers_and_invalid_values():
    """Convert numeric strings and coerce invalid values to NaN."""
    df = pd.DataFrame({
        "open": ["123.45", "bad_value"]
    })

    result = convert_numeric_columns(df, ["open"])

    assert result.loc[0, "open"] == 123.45
    assert pd.isna(result.loc[1, "open"])


def test_remove_duplicates_keeps_first_matching_row():
    """Remove duplicate rows based on the selected subset."""
    df = pd.DataFrame([
        {"symbol": "AAPL", "bar_timestamp": "2026-03-25", "volume": 100},
        {"symbol": "AAPL", "bar_timestamp": "2026-03-25", "volume": 100},
        {"symbol": "MSFT", "bar_timestamp": "2026-03-25", "volume": 200},
    ])

    result = remove_duplicates(df, ["symbol", "bar_timestamp", "volume"])

    assert len(result) == 2


def test_ensure_required_columns_exist_raises_for_missing_columns():
    """Raise a ValueError when a required column is missing."""
    df = pd.DataFrame({
        "symbol": ["AAPL"],
        "open": [100.0]
    })

    with pytest.raises(ValueError, match="Missing required columns"):
        ensure_required_columns_exist(df, ["symbol", "open", "close"])


def test_validate_symbol_rejects_symbol_outside_universe(valid_symbols):
    """Reject a symbol that is not in the approved stock universe."""
    result = validate_symbol("TSLA", valid_symbols)

    assert result == "symbol_not_in_universe"


def test_validate_number_rejects_zero_when_zero_not_allowed():
    """Reject zero for fields that must be strictly positive."""
    result = validate_number(0, "open", allow_zero=False)

    assert result == "non_positive_open"


def test_validate_bar_price_relationships_rejects_high_below_low(valid_bar_row):
    """Reject a stock bar row when the high price is below the low price."""
    row = valid_bar_row.copy()
    row["high"] = 90.0

    result = validate_bar_price_relationships(row)

    assert result == "high_less_than_low"


def test_validate_stock_bar_row_accepts_valid_row(valid_bar_row, valid_symbols):
    """Accept a complete stock bar row when all checks pass."""
    result = validate_stock_bar_row(valid_bar_row, valid_symbols)

    assert result == "valid"


def test_validate_stock_snapshot_row_rejects_price_outside_day_range(valid_snapshot_row, valid_symbols):
    """Reject a snapshot row when the latest trade price is outside the day range."""
    row = valid_snapshot_row.copy()
    row["latest_trade_price"] = 400.0

    result = validate_stock_snapshot_row(row, valid_symbols)

    assert result == "latest_trade_price_outside_current_day_range"


def test_transform_stock_bars_returns_clean_deduplicated_rows(bars_dataframe, valid_symbols):
    """Transform stock bars and return only valid rows after deduplication."""
    result = transform_stock_bars(bars_dataframe, valid_symbols)

    assert len(result) == 2
    assert "validation_result" not in result.columns
    assert list(result["symbol"]) == ["AAPL", "MSFT"]
