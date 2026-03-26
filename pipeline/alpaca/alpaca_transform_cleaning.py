import pandas as pd
from dotenv import load_dotenv

from alpaca_extract import extract_all_stock_data
from top_100_tech_companies import tech_universe
from logger import logger

load_dotenv()


def convert_datetime_columns(df, datetime_columns):
    """
    Convert selected columns to pandas datetime.
    Invalid values become NaT.
    """
    cleaned_df = df.copy()

    for column in datetime_columns:
        if column in cleaned_df.columns:
            cleaned_df[column] = pd.to_datetime(
                cleaned_df[column],
                errors="coerce",
                utc=True
            )

    return cleaned_df


def convert_numeric_columns(df, numeric_columns):
    """
    Convert selected columns to numeric.
    Invalid values become NaN.
    """
    cleaned_df = df.copy()

    for column in numeric_columns:
        if column in cleaned_df.columns:
            cleaned_df[column] = pd.to_numeric(
                cleaned_df[column],
                errors="coerce"
            )

    return cleaned_df


def remove_duplicates(df, subset_columns):
    """
    Remove duplicate rows based on selected columns.
    Keep the first occurrence.
    """
    return df.drop_duplicates(subset=subset_columns, keep="first").copy()


def ensure_required_columns_exist(df, required_columns):
    """
    Raise an error if any required columns are missing.
    """
    missing_columns = []

    for column in required_columns:
        if column not in df.columns:
            missing_columns.append(column)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def validate_symbol(symbol, valid_symbols):
    """
    Check that the symbol exists and belongs to the approved universe.
    """
    if pd.isna(symbol):
        return "missing_symbol"

    if str(symbol).strip() == "":
        return "missing_symbol"

    if symbol not in valid_symbols:
        return "symbol_not_in_universe"

    return "valid"


def validate_timestamp(timestamp_value, column_name):
    """
    Check that a timestamp parsed correctly.
    """
    if pd.isna(timestamp_value):
        return f"invalid_{column_name}"

    return "valid"


def validate_number(value, column_name, allow_zero):
    """
    Validate numeric fields.
    """
    if pd.isna(value):
        return f"missing_{column_name}"

    if allow_zero:
        if value < 0:
            return f"negative_{column_name}"
    else:
        if value <= 0:
            return f"non_positive_{column_name}"

    return "valid"


def validate_vwap_against_range(vwap_value, low_value, high_value, column_name):
    """
    If VWAP is present, check that it lies within the low-high range.
    """
    if pd.isna(vwap_value):
        return "valid"

    if pd.isna(low_value) or pd.isna(high_value):
        return "valid"

    if vwap_value < low_value or vwap_value > high_value:
        return f"{column_name}_outside_price_range"

    return "valid"


def validate_bar_price_relationships(row):
    """
    Validate OHLC relationships for stock bar data.
    """
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


def validate_snapshot_price_relationships(row):
    """
    Validate current day price relationships for stock snapshot data.
    """
    latest_trade_price = row["latest_trade_price"]
    current_open = row["current_day_open"]
    current_high = row["current_day_high"]
    current_low = row["current_day_low"]

    if pd.isna(latest_trade_price) or pd.isna(current_open) or pd.isna(current_high) or pd.isna(current_low):
        return "missing_snapshot_price"

    if current_high < current_low:
        return "current_day_high_less_than_current_day_low"

    if current_high < current_open:
        return "current_day_high_less_than_current_day_open"

    if current_low > current_open:
        return "current_day_low_greater_than_current_day_open"

    if latest_trade_price < current_low or latest_trade_price > current_high:
        return "latest_trade_price_outside_current_day_range"

    return "valid"


def validate_stock_bar_row(row, valid_symbols):
    """
    Validate one stock bar row.
    """
    checks = [
        validate_symbol(row["symbol"], valid_symbols),
        validate_timestamp(row["bar_timestamp"], "bar_timestamp"),
        validate_timestamp(row["bar_date"], "bar_date"),
        validate_number(row["open"], "open", allow_zero=False),
        validate_number(row["high"], "high", allow_zero=False),
        validate_number(row["low"], "low", allow_zero=False),
        validate_number(row["close"], "close", allow_zero=False),
        validate_number(row["volume"], "volume", allow_zero=True),
        validate_number(row["trade_count"], "trade_count", allow_zero=True),
        validate_bar_price_relationships(row),
        validate_vwap_against_range(
            row["vwap"], row["low"], row["high"], "vwap")
    ]

    for result in checks:
        if result != "valid":
            return result

    return "valid"


def validate_stock_snapshot_row(row, valid_symbols):
    """
    Validate one stock snapshot row.
    """
    checks = [
        validate_symbol(row["symbol"], valid_symbols),
        validate_timestamp(row["snapshot_time"], "snapshot_time"),
        validate_number(row["latest_trade_price"],
                        "latest_trade_price", allow_zero=False),
        validate_number(row["previous_close"],
                        "previous_close", allow_zero=False),
        validate_number(row["current_day_open"],
                        "current_day_open", allow_zero=False),
        validate_number(row["current_day_high"],
                        "current_day_high", allow_zero=False),
        validate_number(row["current_day_low"],
                        "current_day_low", allow_zero=False),
        validate_number(row["current_day_volume"],
                        "current_day_volume", allow_zero=True),
        validate_number(row["current_day_trade_count"],
                        "current_day_trade_count", allow_zero=True),
        validate_snapshot_price_relationships(row),
        validate_vwap_against_range(
            row["current_day_vwap"],
            row["current_day_low"],
            row["current_day_high"],
            "current_day_vwap"
        )
    ]

    for result in checks:
        if result != "valid":
            return result

    return "valid"


def run_table_transformation(raw_df, table_name, required_columns,
                             datetime_columns, numeric_columns,
                             duplicate_subset_columns, validation_function,
                             valid_symbols, sort_columns):
    """
    Run the full transformation flow for one table
    and return only the cleaned dataframe.
    """
    logger.info("Starting transformation for %s", table_name)

    working_df = raw_df.copy()

    ensure_required_columns_exist(working_df, required_columns)
    working_df = convert_datetime_columns(working_df, datetime_columns)
    working_df = convert_numeric_columns(working_df, numeric_columns)

    rows_before = len(working_df)
    working_df = remove_duplicates(working_df, duplicate_subset_columns)
    duplicate_rows_removed = rows_before - len(working_df)

    validation_results = []

    for _, row in working_df.iterrows():
        result = validation_function(row, valid_symbols)
        validation_results.append(result)

    working_df["validation_result"] = validation_results

    valid_rows_count = int((working_df["validation_result"] == "valid").sum())
    rejected_rows_count = int(
        (working_df["validation_result"] != "valid").sum())

    cleaned_df = working_df[
        working_df["validation_result"] == "valid"
    ].copy()

    cleaned_df = cleaned_df.drop(columns=["validation_result"])
    cleaned_df = cleaned_df.sort_values(by=sort_columns).reset_index(drop=True)

    logger.info("%s duplicate rows removed: %s",
                table_name, duplicate_rows_removed)
    logger.info("%s valid rows: %s", table_name, valid_rows_count)
    logger.info("%s rejected rows: %s", table_name, rejected_rows_count)

    return cleaned_df


def transform_stock_bars(raw_bars_df, valid_symbols):
    """
    Transform, clean, and validate FACT_STOCK_BARS data.
    """
    return run_table_transformation(
        raw_df=raw_bars_df,
        table_name="FACT_STOCK_BARS",
        required_columns=[
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
            "ingestion_time"
        ],
        datetime_columns=["bar_timestamp", "bar_date", "ingestion_time"],
        numeric_columns=["open", "high", "low",
                         "close", "volume", "trade_count", "vwap"],
        duplicate_subset_columns=["symbol", "bar_timestamp"],
        validation_function=validate_stock_bar_row,
        valid_symbols=valid_symbols,
        sort_columns=["symbol", "bar_timestamp"]
    )


def transform_stock_snapshot(raw_snapshot_df, valid_symbols):
    """
    Transform, clean, and validate FACT_STOCK_SNAPSHOT data.
    """
    return run_table_transformation(
        raw_df=raw_snapshot_df,
        table_name="FACT_STOCK_SNAPSHOT",
        required_columns=[
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
            "ingestion_time"
        ],
        datetime_columns=["snapshot_time", "ingestion_time"],
        numeric_columns=[
            "latest_trade_price",
            "previous_close",
            "current_day_open",
            "current_day_high",
            "current_day_low",
            "current_day_volume",
            "current_day_vwap",
            "current_day_trade_count"
        ],
        duplicate_subset_columns=["symbol", "snapshot_time"],
        validation_function=validate_stock_snapshot_row,
        valid_symbols=valid_symbols,
        sort_columns=["symbol", "snapshot_time"]
    )


def main():
    """
    Run extraction first, then test transformation locally.
    """
    try:
        logger.info("Starting local extraction and transformation test")

        symbols = tech_universe
        start = "2023-03-23"
        end = "2026-03-25"

        extracted_output = extract_all_stock_data(symbols, start, end)

        raw_bars_df = extracted_output["dataframes"]["fact_stock_bars"]
        raw_snapshot_df = extracted_output["dataframes"]["fact_stock_snapshot"]

        clean_bars_df = transform_stock_bars(
            raw_bars_df, symbols)
        clean_snapshot_df = transform_stock_snapshot(
            raw_snapshot_df, symbols)

        logger.info("Bars clean shape: %s", clean_bars_df.shape)

        logger.info("Snapshot clean shape: %s", clean_snapshot_df.shape)

        return {
            "clean_bars_df": clean_bars_df,
            "clean_snapshot_df": clean_snapshot_df,
        }

    except Exception as error:
        logger.exception("Local transformation test failed: %s", error)
        raise ValueError(
            "Data transformation failed during cleaning and validation") from error


if __name__ == "__main__":
    main()
