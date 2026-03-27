"""Module for transforming, cleaning, and validating Alpaca stock data."""

from dataclasses import dataclass
from typing import Any, Callable
import pandas as pd
from dotenv import load_dotenv
from logger import logger
load_dotenv()


def convert_datetime_columns(df: pd.DataFrame,
                             datetime_columns: list[str]) -> pd.DataFrame:
    """Convert selected columns to pandas datetime."""

    cleaned_df = df.copy()

    for column in datetime_columns:
        if column in cleaned_df.columns:
            cleaned_df[column] = pd.to_datetime(
                cleaned_df[column], errors="coerce", utc=True)

    return cleaned_df


def convert_numeric_columns(df: pd.DataFrame,
                            numeric_columns: list[str]) -> pd.DataFrame:
    """Convert selected columns to numeric and invalid values become NaN."""

    cleaned_df = df.copy()

    for column in numeric_columns:
        if column in cleaned_df.columns:
            cleaned_df[column] = pd.to_numeric(
                cleaned_df[column], errors="coerce")

    return cleaned_df


def remove_duplicates(df: pd.DataFrame,
                      subset_columns: list[str]) -> pd.DataFrame:
    """Remove duplicate rows based on selected columns, keep the first occurrence."""

    duplicated = df.drop_duplicates(subset=subset_columns, keep="first").copy()
    return duplicated


def ensure_required_columns_exist(df: pd.DataFrame,
                                  required_columns: list[str]) -> None:
    """Raise an error if any required columns are missing."""
    missing_columns = []

    for column in required_columns:
        if column not in df.columns:
            missing_columns.append(column)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def validate_symbol(symbol: Any, valid_symbols: list[str]) -> str:
    """Check that the symbol exists and belongs to the approved universe."""

    if pd.isna(symbol):
        return "missing_symbol"

    if str(symbol).strip() == "":
        return "missing_symbol"

    if symbol not in valid_symbols:
        return "symbol_not_in_universe"

    return "valid"


def validate_timestamp(timestamp_value: Any, column_name: str) -> str:
    """Check that a timestamp parsed correctly."""

    if pd.isna(timestamp_value):
        return f"invalid_{column_name}"

    return "valid"


def validate_number(value: Any, column_name: str,
                    allow_zero: bool) -> str:
    """Validate numeric fields."""

    if pd.isna(value):
        return f"missing_{column_name}"

    if allow_zero:
        if value < 0:
            return f"negative_{column_name}"
    else:
        if value <= 0:
            return f"non_positive_{column_name}"

    return "valid"


def validate_vwap_against_range(vwap_value: float, low_value: float,
                                high_value: float, column_name: str) -> str:
    """If VWAP is present, check that it lies within the low-high range."""

    if pd.isna(vwap_value):
        return "valid"

    if pd.isna(low_value) or pd.isna(high_value):
        return "valid"

    if vwap_value < low_value or vwap_value > high_value:
        return f"{column_name}_outside_price_range"

    return "valid"


def validate_bar_price_relationships(row: pd.Series) -> str:
    """Validate OHLC relationships for stock bar data."""

    open_price = row["open"]
    high_price = row["high"]
    low_price = row["low"]
    close_price = row["close"]

    if pd.isna(open_price) or pd.isna(high_price) or pd.isna(low_price) or pd.isna(close_price):
        return "missing_bar_price"

    if high_price < low_price:
        return "high_less_than_low"

    if high_price < open_price:
        return "high_less_than_open"

    if high_price < close_price:
        return "high_less_than_close"

    if low_price > open_price:
        return "low_greater_than_open"

    if low_price > close_price:
        return "low_greater_than_close"

    return "valid"


def validate_latest_bar_price_relationships(row: pd.Series) -> str:
    """Validate OHLC relationships for latest bar (snapshot) data."""

    open_price = row["open"]
    high_price = row["high"]
    low_price = row["low"]
    close_price = row["close"]

    if pd.isna(open_price) or pd.isna(high_price) or pd.isna(low_price) or pd.isna(close_price):
        return "missing_bar_price"

    if high_price < low_price:
        return "high_less_than_low"

    if high_price < open_price:
        return "high_less_than_open"

    if high_price < close_price:
        return "high_less_than_close"

    if low_price > open_price:
        return "low_greater_than_open"

    if low_price > close_price:
        return "low_greater_than_close"

    return "valid"


def validate_numeric_fields(
        row: pd.Series,
        fields: list[tuple[str, bool]]) -> str:
    """Validate multiple numeric fields in one pass.

    Each entry in fields is (column_name, allow_zero).
    Returns the first failure reason, or 'valid' if all pass.
    """
    for column_name, allow_zero in fields:
        result = validate_number(row[column_name], column_name, allow_zero)
        if result != "valid":
            return result
    return "valid"


def validate_stock_bar_row(row: pd.Series,
                           valid_symbols: list[str]) -> str:
    """Validate one stock bar row."""

    checks = [
        validate_symbol(row["ticker"], valid_symbols),
        validate_timestamp(row["bar_date"], "bar_date"),
        validate_numeric_fields(row, [
            ("open", False), ("high", False), ("low", False), ("close", False),
            ("volume", True), ("trade_count", True),]),
        validate_bar_price_relationships(row),
        validate_vwap_against_range(row["vwap"], row["low"], row["high"], "vwap")]

    for result in checks:
        if result != "valid":
            return result

    return "valid"


def validate_stock_latest_bar_row(row: pd.Series,
                                  valid_symbols: list[str]) -> str:
    """Validate one stock latest_bar row."""

    checks = [
        validate_symbol(row["ticker"], valid_symbols),
        validate_timestamp(row["latest_time"], "latest_time"),
        validate_numeric_fields(row, [
            ("open", False), ("high", False), ("low", False), ("close", False),
            ("volume", True), ("trade_count", True),]),
        validate_latest_bar_price_relationships(row),
        validate_vwap_against_range(row["vwap"], row["low"], row["high"], "vwap")]

    for result in checks:
        if result != "valid":
            return result

    return "valid"


@dataclass
class TableConfig:
    """Declarative configuration for one table's cleaning pipeline."""
    table_name: str
    required_columns: list[str]
    datetime_columns: list[str]
    numeric_columns: list[str]
    duplicate_subset_columns: list[str]
    validation_function: Callable[[pd.Series, list[str]], str]
    sort_columns: list[str]


def _cast_columns(df: pd.DataFrame,
                  config: TableConfig) -> pd.DataFrame:
    """Apply datetime and numeric type casting defined in the config."""

    working_df = convert_datetime_columns(df, config.datetime_columns)
    working_df = convert_numeric_columns(working_df, config.numeric_columns)
    return working_df


def _deduplicate(df: pd.DataFrame,
                 config: TableConfig) -> tuple[pd.DataFrame, int]:
    """Remove duplicate rows and return the cleaned frame plus the drop count."""

    rows_before = len(df)
    deduped_df = remove_duplicates(df, config.duplicate_subset_columns)
    return deduped_df, rows_before - len(deduped_df)


def _validate_rows(df: pd.DataFrame, config: TableConfig,
                   valid_symbols: list[str]) -> tuple[pd.DataFrame, int, int]:
    """Run the row-level validator and return cleaned rows plus counts."""
    validation_results: list[str] = []

    for _, row in df.iterrows():
        result = config.validation_function(row, valid_symbols)
        validation_results.append(result)

    df["validation_result"] = validation_results

    valid_count = int((df["validation_result"] == "valid").sum())
    rejected_count = int((df["validation_result"] != "valid").sum())

    cleaned_df = df[df["validation_result"] == "valid"].copy()
    cleaned_df = cleaned_df.drop(columns=["validation_result"])

    return cleaned_df, valid_count, rejected_count


def run_table_transformation(raw_df: pd.DataFrame,
                             table_name: str,
                             required_columns: list[str],
                             datetime_columns: list[str],
                             numeric_columns: list[str],
                             duplicate_subset_columns: list[str],
                             validation_function: Callable[[pd.Series, list[str]], str],
                             valid_symbols: list[str],
                             sort_columns: list[str],) -> pd.DataFrame:
    """Run the full transformation flow for one table
    and return only the cleaned dataframe."""

    logger.info("Starting transformation for %s", table_name)

    config = TableConfig(
        table_name=table_name,
        required_columns=required_columns,
        datetime_columns=datetime_columns,
        numeric_columns=numeric_columns,
        duplicate_subset_columns=duplicate_subset_columns,
        validation_function=validation_function,
        sort_columns=sort_columns,)

    working_df = raw_df.copy()

    ensure_required_columns_exist(working_df, config.required_columns)
    working_df = _cast_columns(working_df, config)
    working_df, duplicate_rows_removed = _deduplicate(working_df, config)
    cleaned_df, valid_rows_count, rejected_rows_count = _validate_rows(
        working_df, config, valid_symbols)

    cleaned_df = cleaned_df.sort_values(
        by=config.sort_columns
    ).reset_index(drop=True)

    logger.info("%s duplicate rows removed: %s",
                table_name, duplicate_rows_removed)
    logger.info("%s valid rows: %s", table_name, valid_rows_count)
    logger.info("%s rejected rows: %s", table_name, rejected_rows_count)

    return cleaned_df


def transform_stock_bars(
        raw_bars_df: pd.DataFrame, valid_symbols: list[str]) -> pd.DataFrame:
    """Transform, clean, and validate alpaca_history (stock bars) data."""

    transform_daily_bars = run_table_transformation(
        raw_df=raw_bars_df,
        table_name="ALPACA_HISTORY",
        required_columns=[
            "ticker",
            "bar_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",],
        datetime_columns=["bar_date"],
        numeric_columns=["open", "high", "low",
                         "close", "volume", "trade_count", "vwap"],
        duplicate_subset_columns=["ticker", "bar_date"],
        validation_function=validate_stock_bar_row,
        valid_symbols=valid_symbols,
        sort_columns=["ticker", "bar_date"])

    return transform_daily_bars


def transform_stock_latest_bars(raw_latest_bars_df: pd.DataFrame,
                                valid_symbols: list[str]) -> pd.DataFrame:
    """Transform, clean, and validate alpaca_live (latest bars/snapshot) data."""
    transform_latest = run_table_transformation(
        raw_df=raw_latest_bars_df,
        table_name="ALPACA_LIVE",
        required_columns=[
            "ticker",
            "latest_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
        ],
        datetime_columns=["latest_time"],
        numeric_columns=[
            "close",
            "open",
            "high",
            "low",
            "volume",
            "vwap",
            "trade_count"
        ],
        duplicate_subset_columns=["ticker", "latest_time"],
        validation_function=validate_stock_latest_bar_row,
        valid_symbols=valid_symbols,
        sort_columns=["ticker", "latest_time"]
    )

    return transform_latest


def clean_all_stock_data(extracted_output: dict,
                         valid_symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Clean and validate all extracted stock tables
    and return a dict of cleaned dataframes."""

    logger.info("Starting full cleaning workflow")

    raw_bars_df = extracted_output["dataframes"]["alpaca_history"]
    raw_latest_bars_df = extracted_output["dataframes"]["alpaca_live"]

    clean_bars_df = transform_stock_bars(raw_bars_df, valid_symbols)
    clean_latest_bars_df = transform_stock_latest_bars(
        raw_latest_bars_df, valid_symbols)

    cleaned_data = {
        "alpaca_history": clean_bars_df,
        "alpaca_live": clean_latest_bars_df,
    }

    logger.info("Finished full cleaning workflow")
    return cleaned_data
