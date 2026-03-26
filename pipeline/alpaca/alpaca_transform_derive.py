"""Derive separate feature tables from cleaned Alpaca stock data."""

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from alpaca_extract import extract_all_stock_data
from alpaca_transform_cleaning import transform_stock_bars, transform_stock_snapshot
from top_100_tech_companies import tech_universe
from logger import logger

load_dotenv()


def build_symbol_list(symbol_source):
    """
    Convert the imported symbol source into a plain list of symbols.
    """
    if isinstance(symbol_source, dict):
        return list(symbol_source.keys())

    return list(symbol_source)


def validate_required_columns(df, required_columns, table_name):
    """
    Raise an error if any required columns are missing.
    """
    missing_columns = []

    for column in required_columns:
        if column not in df.columns:
            missing_columns.append(column)

    if missing_columns:
        raise ValueError(
            f"Missing required columns in {table_name}: {missing_columns}"
        )


def prepare_fact_stock_bars(df):
    """
    Validate and cast the cleaned daily bars table before feature engineering.
    """
    required_columns = [
        "symbol",
        "bar_timestamp",
        "bar_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
        "ingestion_time",
    ]

    validate_required_columns(df, required_columns, "fact_stock_bars")

    prepared_df = df.copy()

    prepared_df["bar_timestamp"] = pd.to_datetime(
        prepared_df["bar_timestamp"], errors="coerce", utc=True
    )
    prepared_df["bar_date"] = pd.to_datetime(
        prepared_df["bar_date"], errors="coerce", utc=True
    )
    prepared_df["ingestion_time"] = pd.to_datetime(
        prepared_df["ingestion_time"], errors="coerce", utc=True
    )

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
    ]

    for column in numeric_columns:
        prepared_df[column] = pd.to_numeric(
            prepared_df[column], errors="coerce")

    prepared_df = prepared_df.sort_values(
        by=["symbol", "bar_date"]
    ).reset_index(drop=True)

    return prepared_df


def prepare_fact_stock_snapshot(df):
    """
    Validate and cast the cleaned snapshot table before feature engineering.
    """
    required_columns = [
        "symbol",
        "snapshot_time",
        "latest_trade_price",
        "previous_close",
        "current_day_open",
        "current_day_high",
        "current_day_low",
        "current_day_volume",
        "current_day_vwap",
        "current_day_trade_count",
        "ingestion_time",
    ]

    validate_required_columns(df, required_columns, "fact_stock_snapshot")

    prepared_df = df.copy()

    prepared_df["snapshot_time"] = pd.to_datetime(
        prepared_df["snapshot_time"], errors="coerce", utc=True
    )
    prepared_df["ingestion_time"] = pd.to_datetime(
        prepared_df["ingestion_time"], errors="coerce", utc=True
    )

    numeric_columns = [
        "latest_trade_price",
        "previous_close",
        "current_day_open",
        "current_day_high",
        "current_day_low",
        "current_day_volume",
        "current_day_vwap",
        "current_day_trade_count",
    ]

    for column in numeric_columns:
        prepared_df[column] = pd.to_numeric(
            prepared_df[column], errors="coerce")

    prepared_df = prepared_df.sort_values(
        by=["symbol", "snapshot_time"]
    ).reset_index(drop=True)

    return prepared_df


def add_return_metrics(df):
    """
    Add daily and multi-period return metrics from close prices.
    """
    feature_df = df.copy()
    grouped_close = feature_df.groupby("symbol")["close"]

    feature_df["return_1d"] = grouped_close.pct_change().round(4)
    feature_df["return_5d"] = grouped_close.pct_change(periods=5).round(4)
    feature_df["return_21d"] = grouped_close.pct_change(periods=21).round(4)
    feature_df["return_63d"] = grouped_close.pct_change(periods=63).round(4)
    feature_df["return_126d"] = grouped_close.pct_change(periods=126).round(4)

    return feature_df


def add_trend_metrics(df):
    """
    Add moving averages and trend regime metrics.
    """
    feature_df = df.copy()
    grouped_close = feature_df.groupby("symbol")["close"]

    feature_df["ma_20"] = grouped_close.transform(
        lambda series: series.rolling(window=20, min_periods=20).mean()
    ).round(4)

    feature_df["ma_50"] = grouped_close.transform(
        lambda series: series.rolling(window=50, min_periods=50).mean()
    ).round(4)

    feature_df["ma_200"] = grouped_close.transform(
        lambda series: series.rolling(window=200, min_periods=200).mean()
    ).round(4)

    feature_df["price_vs_ma20"] = (
        (feature_df["close"] / feature_df["ma_20"]) - 1
    ).round(4)

    feature_df["price_vs_ma50"] = (
        (feature_df["close"] / feature_df["ma_50"]) - 1
    ).round(4)

    feature_df["price_vs_ma200"] = (
        (feature_df["close"] / feature_df["ma_200"]) - 1
    ).round(4)

    feature_df["ma50_vs_ma200"] = (
        (feature_df["ma_50"] / feature_df["ma_200"]) - 1
    ).round(4)

    return feature_df


def add_risk_metrics(df):
    """
    Add realised volatility, ATR, and 52 week positioning metrics.
    """
    feature_df = df.copy()

    grouped_close = feature_df.groupby("symbol")["close"]
    grouped_high = feature_df.groupby("symbol")["high"]
    grouped_low = feature_df.groupby("symbol")["low"]

    daily_return = grouped_close.pct_change()

    feature_df["volatility_20d"] = daily_return.groupby(feature_df["symbol"]).transform(
        lambda series: series.rolling(
            window=20, min_periods=20).std() * np.sqrt(252)
    ).round(4)

    feature_df["volatility_60d"] = daily_return.groupby(feature_df["symbol"]).transform(
        lambda series: series.rolling(
            window=60, min_periods=60).std() * np.sqrt(252)
    ).round(4)

    previous_close = grouped_close.shift(1)

    range_1 = feature_df["high"] - feature_df["low"]
    range_2 = (feature_df["high"] - previous_close).abs()
    range_3 = (feature_df["low"] - previous_close).abs()

    true_range_df = pd.concat([range_1, range_2, range_3], axis=1)

    feature_df["true_range"] = true_range_df.max(axis=1).round(4)

    feature_df["atr_20"] = feature_df.groupby("symbol")["true_range"].transform(
        lambda series: series.rolling(window=20, min_periods=20).mean()
    ).round(4)

    feature_df["atr_pct_20"] = (
        feature_df["atr_20"] / feature_df["close"]
    ).round(4)

    feature_df["rolling_high_252d"] = grouped_high.transform(
        lambda series: series.rolling(window=252, min_periods=252).max()
    ).round(4)

    feature_df["rolling_low_252d"] = grouped_low.transform(
        lambda series: series.rolling(window=252, min_periods=252).min()
    ).round(4)

    feature_df["drawdown_252d"] = (
        (feature_df["close"] / feature_df["rolling_high_252d"]) - 1
    ).round(4)

    feature_df["distance_from_252d_low"] = (
        (feature_df["close"] / feature_df["rolling_low_252d"]) - 1
    ).round(4)

    return feature_df


def add_liquidity_metrics(df):
    """
    Add raw and average liquidity metrics using volume and dollar volume.
    """
    feature_df = df.copy()

    grouped_volume = feature_df.groupby("symbol")["volume"]

    feature_df["avg_volume_20d"] = grouped_volume.transform(
        lambda series: series.rolling(window=20, min_periods=20).mean()
    ).round(2)

    feature_df["relative_volume_20d"] = (
        feature_df["volume"] / feature_df["avg_volume_20d"]
    ).round(4)

    feature_df["dollar_volume"] = (
        feature_df["close"] * feature_df["volume"]
    ).round(2)

    feature_df["avg_dollar_volume_20d"] = feature_df.groupby("symbol")["dollar_volume"].transform(
        lambda series: series.rolling(window=20, min_periods=20).mean()
    ).round(2)

    feature_df["relative_dollar_volume_20d"] = (
        feature_df["dollar_volume"] / feature_df["avg_dollar_volume_20d"]
    ).round(4)

    return feature_df


def add_universe_relative_metrics(df):
    """
    Add universe-relative returns and cross-sectional momentum rank.
    """
    feature_df = df.copy()

    market_return_21d = (
        feature_df.groupby("bar_date")["return_21d"]
        .mean()
        .rename("market_return_21d")
        .reset_index()
    )

    market_return_63d = (
        feature_df.groupby("bar_date")["return_63d"]
        .mean()
        .rename("market_return_63d")
        .reset_index()
    )

    feature_df = feature_df.merge(market_return_21d, on="bar_date", how="left")
    feature_df = feature_df.merge(market_return_63d, on="bar_date", how="left")

    feature_df["relative_return_21d"] = (
        feature_df["return_21d"] - feature_df["market_return_21d"]
    ).round(4)

    feature_df["relative_return_63d"] = (
        feature_df["return_63d"] - feature_df["market_return_63d"]
    ).round(4)

    feature_df["momentum_rank_63d"] = feature_df.groupby("bar_date")["return_63d"].rank(
        pct=True
    ).round(4)

    return feature_df


def add_daily_feature_ready_flag(df):
    """
    Flag rows where the main longer-window daily features are available.
    """
    feature_df = df.copy()

    required_feature_columns = [
        "return_21d",
        "return_63d",
        "return_126d",
        "ma_50",
        "ma_200",
        "volatility_20d",
        "volatility_60d",
        "atr_pct_20",
        "drawdown_252d",
        "avg_dollar_volume_20d",
        "relative_return_63d",
        "momentum_rank_63d",
    ]

    feature_df["feature_ready_daily"] = feature_df[required_feature_columns].notna(
    ).all(axis=1)

    return feature_df


def select_daily_feature_table(df):
    """
    Keep only the key columns for the separate daily derived table.
    """
    feature_columns = [
        "symbol",
        "bar_timestamp",
        "bar_date",
        "ingestion_time",
        "return_1d",
        "return_5d",
        "return_21d",
        "return_63d",
        "return_126d",
        "ma_20",
        "ma_50",
        "ma_200",
        "price_vs_ma20",
        "price_vs_ma50",
        "price_vs_ma200",
        "ma50_vs_ma200",
        "volatility_20d",
        "volatility_60d",
        "true_range",
        "atr_20",
        "atr_pct_20",
        "rolling_high_252d",
        "rolling_low_252d",
        "drawdown_252d",
        "distance_from_252d_low",
        "avg_volume_20d",
        "relative_volume_20d",
        "dollar_volume",
        "avg_dollar_volume_20d",
        "relative_dollar_volume_20d",
        "market_return_21d",
        "market_return_63d",
        "relative_return_21d",
        "relative_return_63d",
        "momentum_rank_63d",
        "feature_ready_daily",
    ]

    validate_required_columns(df, feature_columns, "fact_stock_features_daily")

    feature_df = df[feature_columns].copy()
    return feature_df


def transform_fact_stock_bars_features(df):
    """
    Build a separate derived daily feature table.
    """
    logger.info("Starting daily stock feature derivation")

    feature_df = prepare_fact_stock_bars(df)
    feature_df = add_return_metrics(feature_df)
    feature_df = add_trend_metrics(feature_df)
    feature_df = add_risk_metrics(feature_df)
    feature_df = add_liquidity_metrics(feature_df)
    feature_df = add_universe_relative_metrics(feature_df)
    feature_df = add_daily_feature_ready_flag(feature_df)
    feature_df = select_daily_feature_table(feature_df)

    logger.info(
        "Finished daily stock feature derivation. Rows: %s",
        len(feature_df)
    )

    return feature_df


def build_latest_daily_context(daily_feature_df):
    """
    Keep the latest daily feature row for each symbol and rename columns
    so they can be merged onto the snapshot feature table.
    """
    context_columns = [
        "symbol",
        "bar_date",
        "return_21d",
        "return_63d",
        "return_126d",
        "price_vs_ma50",
        "price_vs_ma200",
        "ma50_vs_ma200",
        "volatility_20d",
        "volatility_60d",
        "drawdown_252d",
        "distance_from_252d_low",
        "avg_dollar_volume_20d",
        "relative_return_21d",
        "relative_return_63d",
        "momentum_rank_63d",
        "feature_ready_daily",
    ]

    validate_required_columns(
        daily_feature_df,
        context_columns,
        "fact_stock_features_daily"
    )

    latest_daily_df = daily_feature_df.copy()
    latest_daily_df = latest_daily_df.sort_values(
        by=["symbol", "bar_date"]
    ).reset_index(drop=True)

    latest_daily_df = latest_daily_df.drop_duplicates(
        subset=["symbol"],
        keep="last"
    )

    latest_daily_df = latest_daily_df[context_columns].copy()

    latest_daily_df = latest_daily_df.rename(columns={
        "bar_date": "latest_bar_date",
        "return_21d": "latest_return_21d",
        "return_63d": "latest_return_63d",
        "return_126d": "latest_return_126d",
        "price_vs_ma50": "latest_price_vs_ma50",
        "price_vs_ma200": "latest_price_vs_ma200",
        "ma50_vs_ma200": "latest_ma50_vs_ma200",
        "volatility_20d": "latest_volatility_20d",
        "volatility_60d": "latest_volatility_60d",
        "drawdown_252d": "latest_drawdown_252d",
        "distance_from_252d_low": "latest_distance_from_252d_low",
        "avg_dollar_volume_20d": "latest_avg_dollar_volume_20d",
        "relative_return_21d": "latest_relative_return_21d",
        "relative_return_63d": "latest_relative_return_63d",
        "momentum_rank_63d": "latest_momentum_rank_63d",
    })

    return latest_daily_df


def add_snapshot_intraday_metrics(df):
    """
    Add intraday metrics from the hourly snapshot table.
    """
    feature_df = df.copy()

    feature_df["open_gap_pct"] = (
        (feature_df["current_day_open"] / feature_df["previous_close"]) - 1
    ).round(4)

    feature_df["intraday_return_pct"] = (
        (feature_df["latest_trade_price"] / feature_df["previous_close"]) - 1
    ).round(4)

    feature_df["move_from_open_pct"] = (
        (feature_df["latest_trade_price"] / feature_df["current_day_open"]) - 1
    ).round(4)

    feature_df["day_range_pct"] = (
        (feature_df["current_day_high"] -
         feature_df["current_day_low"]) / feature_df["previous_close"]
    ).round(4)

    feature_df["price_vs_vwap_pct"] = (
        (feature_df["latest_trade_price"] / feature_df["current_day_vwap"]) - 1
    ).round(4)

    feature_df["distance_from_day_high_pct"] = (
        (feature_df["latest_trade_price"] / feature_df["current_day_high"]) - 1
    ).round(4)

    feature_df["distance_from_day_low_pct"] = (
        (feature_df["latest_trade_price"] / feature_df["current_day_low"]) - 1
    ).round(4)

    day_range = feature_df["current_day_high"] - feature_df["current_day_low"]
    safe_day_range = day_range.replace(0, np.nan)

    feature_df["position_in_day_range"] = (
        (feature_df["latest_trade_price"] -
         feature_df["current_day_low"]) / safe_day_range
    ).round(4)

    return feature_df


def merge_snapshot_with_daily_context(snapshot_feature_df, latest_daily_context_df):
    """
    Merge hourly snapshot metrics with the latest daily context.
    """
    merged_df = snapshot_feature_df.copy()

    merged_df = merged_df.merge(
        latest_daily_context_df,
        on="symbol",
        how="left"
    )

    merged_df["days_since_latest_bar"] = (
        merged_df["snapshot_time"].dt.normalize(
        ) - merged_df["latest_bar_date"].dt.normalize()
    ).dt.days

    return merged_df


def add_snapshot_feature_ready_flag(df):
    """
    Flag rows where the main snapshot features and joined context fields exist.
    """
    feature_df = df.copy()

    required_feature_columns = [
        "intraday_return_pct",
        "move_from_open_pct",
        "price_vs_vwap_pct",
        "position_in_day_range",
        "latest_return_63d",
        "latest_price_vs_ma200",
        "latest_volatility_20d",
        "latest_avg_dollar_volume_20d",
    ]

    feature_df["feature_ready_snapshot"] = feature_df[required_feature_columns].notna(
    ).all(axis=1)

    return feature_df


def select_snapshot_feature_table(df):
    """
    Keep only the key columns for the separate snapshot derived table.
    """
    feature_columns = [
        "symbol",
        "snapshot_time",
        "ingestion_time",
        "open_gap_pct",
        "intraday_return_pct",
        "move_from_open_pct",
        "day_range_pct",
        "price_vs_vwap_pct",
        "distance_from_day_high_pct",
        "distance_from_day_low_pct",
        "position_in_day_range",
        "latest_bar_date",
        "days_since_latest_bar",
        "latest_return_21d",
        "latest_return_63d",
        "latest_return_126d",
        "latest_price_vs_ma50",
        "latest_price_vs_ma200",
        "latest_ma50_vs_ma200",
        "latest_volatility_20d",
        "latest_volatility_60d",
        "latest_drawdown_252d",
        "latest_distance_from_252d_low",
        "latest_avg_dollar_volume_20d",
        "latest_relative_return_21d",
        "latest_relative_return_63d",
        "latest_momentum_rank_63d",
        "feature_ready_daily",
        "feature_ready_snapshot",
    ]

    validate_required_columns(
        df, feature_columns, "fact_stock_snapshot_features")

    feature_df = df[feature_columns].copy()
    return feature_df


def transform_fact_stock_snapshot_features(snapshot_df, daily_feature_df):
    """
    Build a separate derived snapshot feature table.
    """
    logger.info("Starting snapshot feature derivation")

    feature_df = prepare_fact_stock_snapshot(snapshot_df)
    feature_df = add_snapshot_intraday_metrics(feature_df)

    latest_daily_context_df = build_latest_daily_context(daily_feature_df)
    feature_df = merge_snapshot_with_daily_context(
        feature_df, latest_daily_context_df)
    feature_df = add_snapshot_feature_ready_flag(feature_df)
    feature_df = select_snapshot_feature_table(feature_df)

    logger.info(
        "Finished snapshot feature derivation. Rows: %s",
        len(feature_df)
    )

    return feature_df


def transform_all_stock_data(cleaned_data):
    """
    Return both original cleaned tables and separate derived tables.
    """
    logger.info("Starting full stock feature workflow")

    try:
        fact_stock_bars_df = cleaned_data["fact_stock_bars"]
        fact_stock_snapshot_df = cleaned_data["fact_stock_snapshot"]

        fact_stock_features_daily_df = transform_fact_stock_bars_features(
            fact_stock_bars_df
        )

        fact_stock_snapshot_features_df = transform_fact_stock_snapshot_features(
            fact_stock_snapshot_df,
            fact_stock_features_daily_df
        )

        output = {
            "fact_stock_bars": fact_stock_bars_df,
            "fact_stock_snapshot": fact_stock_snapshot_df,
            "fact_stock_features_daily": fact_stock_features_daily_df,
            "fact_stock_snapshot_features": fact_stock_snapshot_features_df,
        }

        logger.info("Finished full stock feature workflow")
        return output

    except Exception as error:
        logger.exception("Stock feature workflow failed: %s", error)
        raise


def main():
    """
    Local test harness for extraction, cleaning, and feature derivation.
    """
    try:
        logger.info("Starting local stock feature test")

        symbols = build_symbol_list(tech_universe)

        start = "2023-01-01"
        end = pd.Timestamp.utcnow().date().isoformat()

        extracted_output = extract_all_stock_data(symbols, start, end)

        raw_bars_df = extracted_output["dataframes"]["fact_stock_bars"]
        raw_snapshot_df = extracted_output["dataframes"]["fact_stock_snapshot"]

        clean_bars_df = transform_stock_bars(raw_bars_df, symbols)
        clean_snapshot_df = transform_stock_snapshot(raw_snapshot_df, symbols)

        cleaned_data = {
            "fact_stock_bars": clean_bars_df,
            "fact_stock_snapshot": clean_snapshot_df,
        }

        transformed_output = transform_all_stock_data(cleaned_data)

        logger.info("FACT_STOCK_BARS head:\n%s",
                    transformed_output["fact_stock_bars"].head())
        logger.info("FACT_STOCK_SNAPSHOT head:\n%s",
                    transformed_output["fact_stock_snapshot"].head())
        logger.info("FACT_STOCK_FEATURES_DAILY head:\n%s",
                    transformed_output["fact_stock_features_daily"].head())
        logger.info("FACT_STOCK_SNAPSHOT_FEATURES head:\n%s",
                    transformed_output["fact_stock_snapshot_features"].head())

        logger.info("FACT_STOCK_BARS shape: %s",
                    transformed_output["fact_stock_bars"].shape)
        logger.info("FACT_STOCK_SNAPSHOT shape: %s",
                    transformed_output["fact_stock_snapshot"].shape)
        logger.info("FACT_STOCK_FEATURES_DAILY shape: %s",
                    transformed_output["fact_stock_features_daily"].shape)
        logger.info("FACT_STOCK_SNAPSHOT_FEATURES shape: %s",
                    transformed_output["fact_stock_snapshot_features"].shape)

        return transformed_output

    except Exception as error:
        logger.exception("Local stock feature test failed: %s", error)
        raise ValueError(
            "Stock feature derivation failed during local test"
        ) from error


if __name__ == "__main__":
    main()
