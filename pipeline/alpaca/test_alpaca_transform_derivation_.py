"""Unit tests for Alpaca stock feature derivation functions."""

import numpy as np
import pandas as pd
import pytest

from alpaca_transform_derive import (
    check_required_columns,
    prepare_alpaca_history,
    prepare_alpaca_live,
    add_return_metrics,
    add_trend_metrics,
    add_risk_metrics,
    add_liquidity_metrics,
    add_universe_relative_metrics,
    transform_alpaca_history_features,
    build_latest_daily_context,
    build_previous_close_lookup,
    add_live_intraday_metrics,
    merge_live_with_daily_context,
    transform_alpaca_live_features,
    derive_all_stock_features,
)


@pytest.fixture
def clean_bars_df():
    """Create a cleaned daily bars dataframe with enough history for rolling metrics."""
    rows = []
    dates = pd.bdate_range("2024-01-01", periods=260, tz="UTC")
    tickers = ["AAPL", "MSFT"]

    for ticker_index, ticker in enumerate(tickers):
        for day_index, bar_date in enumerate(dates):
            base_price = 100 + (ticker_index * 20) + day_index

            row = {
                "ticker": ticker,
                "bar_date": bar_date,
                "open": float(base_price),
                "high": float(base_price + 2),
                "low": float(base_price - 2),
                "close": float(base_price + 1),
                "volume": float(1000000 + (day_index * 1000)),
                "trade_count": float(1000 + day_index),
                "vwap": float(base_price + 0.5),
            }
            rows.append(row)

    return pd.DataFrame(rows)


@pytest.fixture
def clean_live_df():
    """Create a cleaned latest minute bar dataframe for the same tickers."""
    rows = [
        {
            "ticker": "AAPL",
            "latest_time": pd.Timestamp("2026-03-26T13:00:00Z"),
            "open": 362.0,
            "high": 368.0,
            "low": 359.0,
            "close": 365.0,
            "volume": 2500000.0,
            "vwap": 363.0,
            "trade_count": 5000.0,
        },
        {
            "ticker": "MSFT",
            "latest_time": pd.Timestamp("2026-03-26T13:00:00Z"),
            "open": 381.0,
            "high": 387.0,
            "low": 379.0,
            "close": 385.0,
            "volume": 2700000.0,
            "vwap": 383.0,
            "trade_count": 5500.0,
        },
    ]

    return pd.DataFrame(rows)


@pytest.fixture
def cleaned_data(clean_bars_df, clean_live_df):
    """Package cleaned daily bars and cleaned live data into one dictionary."""
    return {
        "alpaca_history": clean_bars_df,
        "alpaca_live": clean_live_df,
    }


def test_check_required_columns_raises_when_column_is_missing():
    """Raise an error when a required column is missing from the dataframe."""
    df = pd.DataFrame({
        "ticker": ["AAPL"],
        "close": [100.0],
    })

    with pytest.raises(ValueError, match="Missing required columns"):
        check_required_columns(
            df, ["ticker", "close", "volume"], "alpaca_history")


# Daily history features

def test_prepare_alpaca_history_sorts_rows_by_ticker_and_date(clean_bars_df):
    """Sort the cleaned daily bars table by ticker and bar date."""
    shuffled_df = clean_bars_df.sample(
        frac=1, random_state=42).reset_index(drop=True)

    result = prepare_alpaca_history(shuffled_df)

    assert result.iloc[0]["ticker"] == "AAPL"
    assert result.iloc[-1]["ticker"] == "MSFT"
    assert result.equals(result.sort_values(
        ["ticker", "bar_date"]).reset_index(drop=True))


def test_add_return_metrics_creates_expected_return_columns(clean_bars_df):
    """Add 21 day, 63 day, and 126 day return columns."""
    prepared_df = prepare_alpaca_history(clean_bars_df)

    result = add_return_metrics(prepared_df)

    assert "return_21d" in result.columns
    assert "return_63d" in result.columns
    assert "return_126d" in result.columns
    assert result["return_21d"].notna().sum() > 0


def test_add_trend_metrics_creates_moving_average_columns(clean_bars_df):
    """Add moving-average-based trend columns to the daily bars dataframe."""
    prepared_df = prepare_alpaca_history(clean_bars_df)

    result = add_trend_metrics(prepared_df)

    assert "ma_50" in result.columns
    assert "ma_200" in result.columns
    assert "price_vs_ma50" in result.columns
    assert "price_vs_ma200" in result.columns
    assert result["price_vs_ma200"].notna().sum() > 0


def test_add_risk_metrics_creates_volatility_and_drawdown_columns(clean_bars_df):
    """Add volatility and 52 week drawdown columns to the daily bars dataframe."""
    prepared_df = prepare_alpaca_history(clean_bars_df)

    result = add_risk_metrics(prepared_df)

    assert "volatility_20d" in result.columns
    assert "drawdown_252d" in result.columns
    assert result["volatility_20d"].notna().sum() > 0
    assert result["drawdown_252d"].notna().sum() > 0


def test_add_liquidity_metrics_creates_dollar_volume_columns(clean_bars_df):
    """Add dollar volume and average dollar volume columns."""
    prepared_df = prepare_alpaca_history(clean_bars_df)

    result = add_liquidity_metrics(prepared_df)

    assert "dollar_volume" in result.columns
    assert "avg_dollar_volume_20d" in result.columns
    assert result["avg_dollar_volume_20d"].notna().sum() > 0


def test_add_universe_relative_metrics_creates_relative_strength_columns(clean_bars_df):
    """Add universe-relative return and momentum rank columns."""
    prepared_df = prepare_alpaca_history(clean_bars_df)
    prepared_df = add_return_metrics(prepared_df)

    result = add_universe_relative_metrics(prepared_df)

    assert "market_return_63d" in result.columns
    assert "relative_return_63d" in result.columns
    assert "momentum_rank_63d" in result.columns
    assert result["momentum_rank_63d"].notna().sum() > 0


def test_transform_alpaca_history_features_returns_expected_columns(clean_bars_df):
    """Build the final daily feature table with only the selected columns."""
    result = transform_alpaca_history_features(clean_bars_df)

    expected_columns = [
        "ticker",
        "bar_date",
        "return_21d",
        "return_63d",
        "return_126d",
        "price_vs_ma50",
        "price_vs_ma200",
        "volatility_20d",
        "drawdown_252d",
        "avg_dollar_volume_20d",
        "relative_return_63d",
        "momentum_rank_63d",
        "feature_ready_daily",
    ]

    assert list(result.columns) == expected_columns
    assert len(result) == len(clean_bars_df)
    assert result["feature_ready_daily"].isin([True, False]).all()


def test_build_latest_daily_context_returns_one_row_per_ticker(clean_bars_df):
    """Keep only the latest daily context row for each ticker."""
    daily_features_df = transform_alpaca_history_features(clean_bars_df)

    result = build_latest_daily_context(daily_features_df)

    assert len(result) == 2
    assert sorted(result["ticker"].tolist()) == ["AAPL", "MSFT"]
    assert "latest_return_63d" in result.columns
    assert "latest_price_vs_ma200" in result.columns
    assert "latest_bar_date" in result.columns


# Live features

def test_prepare_alpaca_live_sorts_by_ticker_and_latest_time(clean_live_df):
    """Sort the cleaned latest minute bar data by ticker and latest_time."""
    shuffled_df = clean_live_df.sample(
        frac=1, random_state=42).reset_index(drop=True)

    result = prepare_alpaca_live(shuffled_df)

    assert result.iloc[0]["ticker"] == "AAPL"
    assert result.iloc[-1]["ticker"] == "MSFT"


def test_build_previous_close_lookup_returns_one_row_per_ticker(clean_bars_df):
    """Return the most recent daily close per ticker."""
    result = build_previous_close_lookup(clean_bars_df)

    assert len(result) == 2
    assert sorted(result["ticker"].tolist()) == ["AAPL", "MSFT"]
    assert "previous_close" in result.columns
    assert "close" not in result.columns


def test_add_live_intraday_metrics_creates_expected_columns(clean_live_df, clean_bars_df):
    """Add intraday metrics from latest minute bar and previous close."""
    prepared_df = prepare_alpaca_live(clean_live_df)
    prev_close_df = build_previous_close_lookup(clean_bars_df)
    prepared_df = prepared_df.merge(prev_close_df, on="ticker", how="left")

    result = add_live_intraday_metrics(prepared_df)

    assert "change_from_prev_close_pct" in result.columns
    assert "move_from_open_pct" in result.columns
    assert "price_vs_vwap_pct" in result.columns
    assert "bar_range_pct" in result.columns
    assert "position_in_bar_range" in result.columns


def test_add_live_intraday_metrics_values_are_reasonable(clean_live_df, clean_bars_df):
    """Verify derived intraday metrics produce sensible values."""
    prepared_df = prepare_alpaca_live(clean_live_df)
    prev_close_df = build_previous_close_lookup(clean_bars_df)
    prepared_df = prepared_df.merge(prev_close_df, on="ticker", how="left")

    result = add_live_intraday_metrics(prepared_df)

    # position_in_bar_range should be between 0 and 1
    assert (result["position_in_bar_range"].dropna() >= 0).all()
    assert (result["position_in_bar_range"].dropna() <= 1).all()

    # bar_range_pct should be non-negative
    assert (result["bar_range_pct"].dropna() >= 0).all()


def test_merge_live_with_daily_context_adds_daily_columns(
        clean_live_df, clean_bars_df):
    """Merge live features with the latest daily context and add days_since."""
    daily_features_df = transform_alpaca_history_features(clean_bars_df)
    latest_context_df = build_latest_daily_context(daily_features_df)

    prepared_df = prepare_alpaca_live(clean_live_df)

    result = merge_live_with_daily_context(prepared_df, latest_context_df)

    assert "latest_bar_date" in result.columns
    assert "days_since_latest_bar" in result.columns
    assert "latest_return_63d" in result.columns
    assert "latest_price_vs_ma200" in result.columns
    assert len(result) == len(clean_live_df)


def test_transform_alpaca_live_features_returns_expected_columns(
        clean_live_df, clean_bars_df):
    """Build the final live feature table with intraday and daily context."""
    daily_features_df = transform_alpaca_history_features(clean_bars_df)

    result = transform_alpaca_live_features(
        clean_live_df, clean_bars_df, daily_features_df)

    expected_columns = [
        "ticker",
        "latest_time",
        "previous_close",
        "close",
        "change_from_prev_close_pct",
        "move_from_open_pct",
        "price_vs_vwap_pct",
        "bar_range_pct",
        "position_in_bar_range",
        "latest_bar_date",
        "days_since_latest_bar",
        "latest_return_63d",
        "latest_price_vs_ma50",
        "latest_price_vs_ma200",
        "latest_volatility_20d",
        "latest_drawdown_252d",
        "latest_avg_dollar_volume_20d",
        "latest_relative_return_63d",
        "latest_momentum_rank_63d",
        "feature_ready_daily",
        "feature_ready_live",
    ]

    assert list(result.columns) == expected_columns
    assert len(result) == len(clean_live_df)
    assert result["feature_ready_live"].isin([True, False]).all()


def test_derive_all_stock_features_returns_both_feature_tables(cleaned_data):
    """Return both the daily feature table and the live feature table."""
    result = derive_all_stock_features(cleaned_data)

    assert "alpaca_history_features" in result
    assert "alpaca_live_features" in result
    assert len(result["alpaca_history_features"]) == len(
        cleaned_data["alpaca_history"])
    assert len(result["alpaca_live_features"]) == len(
        cleaned_data["alpaca_live"])
