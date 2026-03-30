"""Derive feature tables from cleaned Alpaca stock data.

"""

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from alpaca_extract import extract_all_stock_data
from alpaca_transform_cleaning import clean_all_stock_data
from top_100_tech_companies import tech_universe
from logger import logger

load_dotenv()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def check_required_columns(df, required_columns, table_name):
    """Raise ValueError when any expected columns are missing."""
    missing_columns = [
        col for col in required_columns if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns in {table_name}: {missing_columns}"
        )


# ---------------------------------------------------------------------------
# Daily history preparation
# ---------------------------------------------------------------------------

def prepare_alpaca_history(df):
    """Sort cleaned daily bars by ticker and date for rolling calculations."""
    required_columns = [
        "ticker", "bar_date", "open", "high", "low",
        "close", "volume", "trade_count", "vwap",
    ]

    check_required_columns(df, required_columns, "alpaca_history")

    prepared_df = df.copy()
    prepared_df = prepared_df.sort_values(
        by=["ticker", "bar_date"]
    ).reset_index(drop=True)

    return prepared_df


# ---------------------------------------------------------------------------
# Daily feature: multi-horizon returns
# ---------------------------------------------------------------------------

def add_return_metrics(df):
    """Add 1-month, 1-quarter, and half-year return metrics."""
    feature_df = df.copy()
    grouped_close = feature_df.groupby("ticker")["close"]

    feature_df["return_21d"] = grouped_close.pct_change(periods=21).round(4)
    feature_df["return_63d"] = grouped_close.pct_change(periods=63).round(4)
    feature_df["return_126d"] = grouped_close.pct_change(periods=126).round(4)

    return feature_df


# ---------------------------------------------------------------------------
# Daily feature: trend / moving averages
# ---------------------------------------------------------------------------

def add_trend_metrics(df):
    """Add 50-day and 200-day moving averages and price distance from each."""
    feature_df = df.copy()
    grouped_close = feature_df.groupby("ticker")["close"]

    feature_df["ma_50"] = grouped_close.transform(
        lambda s: s.rolling(window=50, min_periods=50).mean()
    ).round(4)

    feature_df["ma_200"] = grouped_close.transform(
        lambda s: s.rolling(window=200, min_periods=200).mean()
    ).round(4)

    feature_df["price_vs_ma50"] = (
        (feature_df["close"] / feature_df["ma_50"]) - 1
    ).round(4)

    feature_df["price_vs_ma200"] = (
        (feature_df["close"] / feature_df["ma_200"]) - 1
    ).round(4)

    return feature_df


# ---------------------------------------------------------------------------
# Daily feature: risk
# ---------------------------------------------------------------------------

def add_risk_metrics(df):
    """Add annualised volatility and max-drawdown from the 252-day high."""
    feature_df = df.copy()
    grouped_close = feature_df.groupby("ticker")["close"]
    grouped_high = feature_df.groupby("ticker")["high"]

    daily_return = grouped_close.pct_change()

    feature_df["volatility_20d"] = daily_return.groupby(
        feature_df["ticker"]
    ).transform(
        lambda s: s.rolling(window=20, min_periods=20).std() * (252 ** 0.5)
    ).round(4)

    feature_df["rolling_high_252d"] = grouped_high.transform(
        lambda s: s.rolling(window=252, min_periods=252).max()
    ).round(4)

    feature_df["drawdown_252d"] = (
        (feature_df["close"] / feature_df["rolling_high_252d"]) - 1
    ).round(4)

    return feature_df


# ---------------------------------------------------------------------------
# Daily feature: liquidity
# ---------------------------------------------------------------------------

def add_liquidity_metrics(df):
    """Add average 20-day dollar volume as a liquidity proxy."""
    feature_df = df.copy()

    feature_df["dollar_volume"] = (
        feature_df["close"] * feature_df["volume"]
    ).round(2)

    feature_df["avg_dollar_volume_20d"] = feature_df.groupby(
        "ticker"
    )["dollar_volume"].transform(
        lambda s: s.rolling(window=20, min_periods=20).mean()
    ).round(2)

    return feature_df


# ---------------------------------------------------------------------------
# Daily feature: universe-relative performance
# ---------------------------------------------------------------------------

def add_universe_relative_metrics(df):
    """Add relative return vs equal-weight tech universe and momentum rank."""
    feature_df = df.copy()

    market_return_63d = (
        feature_df.groupby("bar_date")["return_63d"]
        .mean()
        .rename("market_return_63d")
        .reset_index()
    )

    feature_df = feature_df.merge(
        market_return_63d, on="bar_date", how="left"
    )

    feature_df["relative_return_63d"] = (
        feature_df["return_63d"] - feature_df["market_return_63d"]
    ).round(4)

    feature_df["momentum_rank_63d"] = feature_df.groupby(
        "bar_date"
    )["return_63d"].rank(pct=True).round(4)

    return feature_df


# ---------------------------------------------------------------------------
# Daily feature: readiness flag + column selection
# ---------------------------------------------------------------------------

def add_daily_feature_ready_flag(df):
    """Flag rows where every core daily feature is present (not NaN)."""
    feature_df = df.copy()

    required_feature_columns = [
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
    ]

    feature_df["feature_ready_daily"] = (
        feature_df[required_feature_columns].notna().all(axis=1)
    )

    return feature_df


def select_daily_feature_table(df):
    """Keep only the final daily feature columns."""
    feature_columns = [
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

    check_required_columns(df, feature_columns, "alpaca_history_features")

    return df[feature_columns].copy()


# ---------------------------------------------------------------------------
# Daily orchestrator
# ---------------------------------------------------------------------------

def transform_alpaca_history_features(df):
    """Build the daily derived feature table from cleaned history bars."""
    logger.info("Starting daily stock feature derivation")

    feature_df = prepare_alpaca_history(df)
    feature_df = add_return_metrics(feature_df)
    feature_df = add_trend_metrics(feature_df)
    feature_df = add_risk_metrics(feature_df)
    feature_df = add_liquidity_metrics(feature_df)
    feature_df = add_universe_relative_metrics(feature_df)
    feature_df = add_daily_feature_ready_flag(feature_df)
    feature_df = select_daily_feature_table(feature_df)

    logger.info(
        "Finished daily stock feature derivation. Rows: %s", len(feature_df)
    )
    return feature_df


# ---------------------------------------------------------------------------
# Live feature: build daily context to join onto the latest minute bar
# ---------------------------------------------------------------------------

def build_latest_daily_context(daily_feature_df):
    """Extract the most recent daily feature row per ticker for live join."""
    context_columns = [
        "ticker",
        "bar_date",
        "return_63d",
        "price_vs_ma50",
        "price_vs_ma200",
        "volatility_20d",
        "drawdown_252d",
        "avg_dollar_volume_20d",
        "relative_return_63d",
        "momentum_rank_63d",
        "feature_ready_daily",
    ]

    check_required_columns(
        daily_feature_df, context_columns, "alpaca_history_features"
    )

    latest_df = daily_feature_df.copy()
    latest_df = latest_df.sort_values(
        by=["ticker", "bar_date"]
    ).reset_index(drop=True)
    latest_df = latest_df.drop_duplicates(subset=["ticker"], keep="last")
    latest_df = latest_df[context_columns].copy()

    latest_df = latest_df.rename(columns={
        "bar_date": "latest_bar_date",
        "return_63d": "latest_return_63d",
        "price_vs_ma50": "latest_price_vs_ma50",
        "price_vs_ma200": "latest_price_vs_ma200",
        "volatility_20d": "latest_volatility_20d",
        "drawdown_252d": "latest_drawdown_252d",
        "avg_dollar_volume_20d": "latest_avg_dollar_volume_20d",
        "relative_return_63d": "latest_relative_return_63d",
        "momentum_rank_63d": "latest_momentum_rank_63d",
    })

    return latest_df


# ---------------------------------------------------------------------------
# Live feature: get the previous daily close per ticker for live join
# ---------------------------------------------------------------------------

def build_previous_close_lookup(cleaned_history_df):
    """Get the most recent daily close per ticker for intraday calculations."""
    lookup_df = cleaned_history_df.copy()
    lookup_df = lookup_df.sort_values(
        by=["ticker", "bar_date"]
    ).reset_index(drop=True)
    lookup_df = lookup_df.drop_duplicates(subset=["ticker"], keep="last")

    return lookup_df[["ticker", "close"]].rename(
        columns={"close": "previous_close"}
    )


# ---------------------------------------------------------------------------
# Live feature: prepare latest minute bar
# ---------------------------------------------------------------------------

def prepare_alpaca_live(df):
    """Sort the cleaned latest minute bar data for feature engineering."""
    required_columns = [
        "ticker", "latest_time", "open", "high", "low",
        "close", "volume", "vwap", "trade_count",
    ]

    check_required_columns(df, required_columns, "alpaca_live")

    prepared_df = df.copy()
    prepared_df = prepared_df.sort_values(
        by=["ticker", "latest_time"]
    ).reset_index(drop=True)

    return prepared_df


# ---------------------------------------------------------------------------
# Live feature: intraday metrics derived from minute bar + previous close
# ---------------------------------------------------------------------------

def add_live_intraday_metrics(df):
    """Derive intraday metrics from the latest minute bar and previous close.

    Requires a 'previous_close' column already merged onto the dataframe.
    Metrics:
    - change_from_prev_close_pct : overnight + intraday move
    - move_from_open_pct         : how far price moved since bar open
    - price_vs_vwap_pct          : premium/discount to VWAP
    - bar_range_pct              : high-low spread as % of low (volatility proxy)
    - position_in_bar_range      : where close sits within the bar range [0-1]
    """
    feature_df = df.copy()

    # Change from previous daily close to latest close
    feature_df["change_from_prev_close_pct"] = (
        (feature_df["close"] / feature_df["previous_close"]) - 1
    ).round(4)

    # How far the latest bar moved from its own open
    feature_df["move_from_open_pct"] = (
        (feature_df["close"] / feature_df["open"]) - 1
    ).round(4)

    # Premium or discount relative to bar VWAP
    feature_df["price_vs_vwap_pct"] = (
        (feature_df["close"] / feature_df["vwap"]) - 1
    ).round(4)

    # Bar high-low spread as percentage of low (intrabar volatility)
    feature_df["bar_range_pct"] = (
        (feature_df["high"] - feature_df["low"]) / feature_df["low"]
    ).round(4)

    # Where close sits within the bar range (0 = at low, 1 = at high)
    bar_range = feature_df["high"] - feature_df["low"]
    safe_bar_range = bar_range.replace(0, np.nan)

    feature_df["position_in_bar_range"] = (
        (feature_df["close"] - feature_df["low"]) / safe_bar_range
    ).round(4)

    return feature_df


# ---------------------------------------------------------------------------
# Live feature: merge with daily context
# ---------------------------------------------------------------------------

def merge_live_with_daily_context(live_feature_df, latest_daily_context_df):
    """Join live features with the latest daily historical context."""
    merged_df = live_feature_df.merge(
        latest_daily_context_df, on="ticker", how="left"
    )

    merged_df["days_since_latest_bar"] = (
        merged_df["latest_time"].dt.normalize()
        - merged_df["latest_bar_date"].dt.normalize()
    ).dt.days

    return merged_df


# ---------------------------------------------------------------------------
# Live feature: readiness flag + column selection
# ---------------------------------------------------------------------------

def add_live_feature_ready_flag(df):
    """Flag rows where the core live features and daily context are present."""
    feature_df = df.copy()

    required_feature_columns = [
        "change_from_prev_close_pct",
        "move_from_open_pct",
        "price_vs_vwap_pct",
        "latest_return_63d",
        "latest_price_vs_ma200",
        "latest_volatility_20d",
    ]

    feature_df["feature_ready_live"] = (
        feature_df[required_feature_columns].notna().all(axis=1)
    )

    return feature_df


def select_live_feature_table(df):
    """Keep only the final live feature columns."""
    feature_columns = [
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

    check_required_columns(df, feature_columns, "alpaca_live_features")

    return df[feature_columns].copy()


# ---------------------------------------------------------------------------
# Live orchestrator
# ---------------------------------------------------------------------------

def transform_alpaca_live_features(live_df, cleaned_history_df,
                                   daily_feature_df):
    """Build the live derived feature table from the latest minute bar.

    Parameters
    ----------
    live_df : pd.DataFrame
        Cleaned latest minute bar data.
    cleaned_history_df : pd.DataFrame
        Cleaned daily bars (needed for previous close lookup).
    daily_feature_df : pd.DataFrame
        Already-derived daily features (for historical context join).
    """
    logger.info("Starting live feature derivation")

    feature_df = prepare_alpaca_live(live_df)

    # Merge previous daily close for intraday calculations
    prev_close_df = build_previous_close_lookup(cleaned_history_df)
    feature_df = feature_df.merge(prev_close_df, on="ticker", how="left")

    feature_df = add_live_intraday_metrics(feature_df)

    # Merge historical daily context
    latest_daily_context_df = build_latest_daily_context(daily_feature_df)
    feature_df = merge_live_with_daily_context(
        feature_df, latest_daily_context_df
    )

    feature_df = add_live_feature_ready_flag(feature_df)
    feature_df = select_live_feature_table(feature_df)

    logger.info(
        "Finished live feature derivation. Rows: %s", len(feature_df)
    )
    return feature_df


# ---------------------------------------------------------------------------
# Full derivation workflow
# ---------------------------------------------------------------------------

def derive_all_stock_features(cleaned_data):
    """Derive all feature tables from already-cleaned data.

    Expected cleaned_data structure:
    {
        "alpaca_history": pd.DataFrame,
        "alpaca_live": pd.DataFrame,
    }
    """
    logger.info("Starting full stock feature workflow")

    cleaned_history_df = cleaned_data["alpaca_history"]
    cleaned_live_df = cleaned_data["alpaca_live"]

    history_features_df = transform_alpaca_history_features(cleaned_history_df)

    live_features_df = transform_alpaca_live_features(
        cleaned_live_df, cleaned_history_df, history_features_df
    )

    derived_output = {
        "alpaca_history_features": history_features_df,
        "alpaca_live_features": live_features_df,
    }

    logger.info("Finished full stock feature workflow")
    return derived_output


# ---------------------------------------------------------------------------
# Local smoke test
# ---------------------------------------------------------------------------

def main():
    """Run extraction -> cleaning -> derivation locally for testing."""
    try:
        logger.info("Starting local stock feature test")

        symbols = list(tech_universe.keys())
        start = "2023-01-01"
        end = pd.Timestamp.utcnow().date().isoformat()

        extracted_output = extract_all_stock_data(symbols, start, end)
        cleaned_data = clean_all_stock_data(extracted_output, symbols)
        derived_output = derive_all_stock_features(cleaned_data)

        logger.info(
            "ALPACA_HISTORY_FEATURES shape: %s",
            derived_output["alpaca_history_features"].shape,
        )
        logger.info(
            "ALPACA_LIVE_FEATURES shape: %s",
            derived_output["alpaca_live_features"].shape,
        )

        logger.info(
            "ALPACA_HISTORY_FEATURES head:\n%s",
            derived_output["alpaca_history_features"].head(),
        )
        logger.info(
            "ALPACA_LIVE_FEATURES head:\n%s",
            derived_output["alpaca_live_features"].head(),
        )

        return derived_output

    except Exception as error:
        logger.exception("Local stock feature test failed: %s", error)
        raise ValueError(
            "Stock feature derivation failed during local test"
        ) from error


if __name__ == "__main__":
    main()
