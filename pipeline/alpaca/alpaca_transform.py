"""Transform extracted Alpaca stock bar data into MVP analytics-ready DataFrames."""

import logging
import numpy as np
import pandas as pd
from pipeline.alpaca_pipeline.alpaca.logger import logger


def validate_required_columns(df: pd.DataFrame, required_columns: list[str], table_name: str) -> None:
    """Check that all required columns exist in the DataFrame."""
    missing_columns = []

    for column in required_columns:
        if column not in df.columns:
            missing_columns.append(column)

    if missing_columns:
        logger.error(
            "Missing required columns in %s: %s", table_name, missing_columns)

        raise ValueError("Missing required columns in %s: %s" %
                         (table_name, missing_columns))


def cast_fact_stock_bars_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast fact_stock_bars columns to appropriate pandas types."""
    required_columns = [
        "symbol", "bar_timestamp", "bar_date", "open", "high", "low",
        "close", "volume", "trade_count", "vwap", "ingestion_time"
    ]
    validate_required_columns(df, required_columns, "fact_stock_bars")

    logger.info("Casting fact_stock_bars column types")

    df = df.copy()

    df["bar_timestamp"] = pd.to_datetime(df["bar_timestamp"], errors="coerce")
    df["bar_date"] = pd.to_datetime(df["bar_date"], errors="coerce")
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["trade_count"] = pd.to_numeric(df["trade_count"], errors="coerce")
    df["vwap"] = pd.to_numeric(df["vwap"], errors="coerce")
    df["ingestion_time"] = pd.to_datetime(
        df["ingestion_time"], errors="coerce")

    return df


def sort_fact_stock_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Sort the stock bars DataFrame by symbol and bar date."""
    logger.info("Sorting fact_stock_bars by symbol and bar_date")

    df = df.copy()
    df = df.sort_values(by=["symbol", "bar_date"]).reset_index(drop=True)

    return df


def add_return_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 1 month and 3 month returns.

    1 month is approximated as 21 trading days.
    3 months is approximated as 63 trading days.
    """
    logger.info("Adding return metrics")

    df = df.copy()
    grouped_close = df.groupby("symbol")["close"]

    df["return_1m"] = grouped_close.pct_change(periods=21).round(4)
    df["return_3m"] = grouped_close.pct_change(periods=63).round(4)

    return df


def add_moving_average_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add 50 day moving average and price versus moving average."""
    logger.info("Adding moving average metrics")

    df = df.copy()
    grouped_close = df.groupby("symbol")["close"]

    df["ma_50"] = grouped_close.transform(
        lambda series: series.rolling(window=50).mean()
    )
    df["price_vs_ma_50"] = (df["close"] / df["ma_50"]).round(4)

    return df


def add_volatility_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add 20 day annualised volatility."""
    logger.info("Adding volatility metrics")

    df = df.copy()
    grouped_close = df.groupby("symbol")["close"]

    daily_return = grouped_close.pct_change()

    df["volatility_20d"] = daily_return.groupby(df["symbol"]).transform(
        lambda series: series.rolling(window=20).std() * np.sqrt(252)
    ).round(4)

    return df


def add_drawdown_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add drawdown from the running peak."""
    logger.info("Adding drawdown metrics")

    df = df.copy()
    grouped_close = df.groupby("symbol")["close"]

    rolling_peak = grouped_close.transform(lambda series: series.cummax())
    df["drawdown"] = ((df["close"] - rolling_peak) / rolling_peak).round(4)

    return df


def add_relative_performance_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add 1 month relative performance versus the equal-weight universe average."""
    logger.info("Adding relative performance metrics")

    df = df.copy()

    market_return_1m = (
        df.groupby("bar_date")["return_1m"]
        .mean()
        .rename("market_return_1m")
        .reset_index()
    ).round(4)

    df = df.merge(market_return_1m, on="bar_date", how="left")
    df["relative_return_1m"] = (
        df["return_1m"] - df["market_return_1m"]).round(4)

    return df


def add_volume_signal_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add 20 day average volume and relative volume."""
    logger.info("Adding volume signal metrics")

    df = df.copy()
    grouped_volume = df.groupby("symbol")["volume"]

    df["avg_volume_20d"] = grouped_volume.transform(
        lambda series: series.rolling(window=20).mean()
    )
    df["relative_volume_20d"] = (df["volume"] / df["avg_volume_20d"]).round(4)

    return df


def filter_dashboard_ready_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows where the main MVP dashboard metrics are available."""
    logger.info("Filtering dashboard-ready rows")

    required_metric_columns = [
        "return_1m",
        "return_3m",
        "price_vs_ma_50",
        "volatility_20d",
        "drawdown",
        "relative_return_1m",
        "relative_volume_20d"
    ]

    filtered_df = df.dropna(subset=required_metric_columns).copy()

    logger.info(
        "Filtered dashboard-ready rows. Rows before: %s, rows after: %s",
        len(df),
        len(filtered_df)
    )

    return filtered_df


def transform_fact_stock_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Run the full MVP transformation pipeline for fact_stock_bars."""
    logger.info("Starting fact_stock_bars MVP transformation")

    df = cast_fact_stock_bars_types(df)
    df = sort_fact_stock_bars(df)
    df = add_return_metrics(df)
    df = add_moving_average_metrics(df)
    df = add_volatility_metrics(df)
    df = add_drawdown_metrics(df)
    df = add_relative_performance_metrics(df)
    df = add_volume_signal_metrics(df)
    df = filter_dashboard_ready_rows(df)

    logger.info(
        "Finished fact_stock_bars MVP transformation. Total rows: %s",
        len(df)
    )
    return df


def transform_all_stock_data(extracted_data: dict) -> dict:
    """Transform extracted stock DataFrames for loading to S3."""
    logger.info("Starting full stock data transformation workflow")

    fact_stock_bars_df = extracted_data["fact_stock_bars"]
    fact_stock_snapshot_df = extracted_data["fact_stock_snapshot"]

    transformed_fact_stock_bars_df = transform_fact_stock_bars(
        fact_stock_bars_df)

    output = {
        "fact_stock_features": transformed_fact_stock_bars_df,
        "fact_stock_snapshot": fact_stock_snapshot_df
    }

    logger.info("Finished full stock data transformation workflow")
    return output
