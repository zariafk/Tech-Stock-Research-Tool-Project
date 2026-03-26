"""Unit tests for alpaca_extract.py functions."""

from unittest.mock import Mock, patch
import pandas as pd
import pytest
import requests


from alpaca_extract import (
    make_request,
    validate_symbols,
    build_bars_params,
    build_snapshot_params,
    parse_bar_row,
    parse_snapshot_row,
    extract_bar_rows_from_response,
    extract_fact_daily_stock_bars,
    extract_fact_stock_snapshot,
    rows_to_dataframe,
    extract_all_stock_data
)


# Fixtures: shared test data
INGESTION_TIMESTAMP = "2026-03-25T12:00:00Z"
START_DATE = "2026-03-01"
END_DATE = "2026-03-25"


@pytest.fixture()
def aapl_raw_bar():
    """Single AAPL bar as returned by the Alpaca API."""
    return {
        "t": "2026-03-24T00:00:00Z",
        "o": 210.0,
        "h": 215.0,
        "l": 208.0,
        "c": 214.0,
        "v": 1000000,
        "n": 25000,
        "vw": 212.5,
    }


@pytest.fixture()
def aapl_raw_bar_prev_day():
    """AAPL bar for the previous trading day."""
    return {
        "t": "2026-03-23T00:00:00Z",
        "o": 200.0,
        "h": 205.0,
        "l": 198.0,
        "c": 204.0,
        "v": 800000,
        "n": 22000,
        "vw": 202.1,
    }


@pytest.fixture()
def msft_raw_bar():
    """Single MSFT bar as returned by the Alpaca API."""
    return {
        "t": "2026-03-24T00:00:00Z",
        "o": 300.0,
        "h": 305.0,
        "l": 298.0,
        "c": 304.0,
        "v": 900000,
        "n": 19000,
        "vw": 302.1,
    }


@pytest.fixture()
def aapl_raw_snapshot():
    """AAPL snapshot as returned by the Alpaca API."""
    return {
        "latestTrade": {"t": "2026-03-25T11:30:00Z", "p": 220.5},
        "dailyBar": {
            "o": 218.0,
            "h": 221.0,
            "l": 217.5,
            "v": 500000,
            "vw": 219.4,
            "n": 14000,
        },
        "prevDailyBar": {"c": 217.8},
    }


@pytest.fixture()
def msft_raw_snapshot():
    """MSFT snapshot as returned by the Alpaca API."""
    return {
        "latestTrade": {"t": "2026-03-25T11:31:00Z", "p": 330.5},
        "dailyBar": {
            "o": 328.0,
            "h": 332.0,
            "l": 327.0,
            "v": 450000,
            "vw": 329.4,
            "n": 12000,
        },
        "prevDailyBar": {"c": 327.8},
    }


# Helper functions for tests
def test_validate_symbols_with_non_empty_list():
    """validate_symbols should do nothing when symbols are provided."""
    validate_symbols(["AAPL", "MSFT"], "fact_stock_bars")


def test_validate_symbols_with_empty_list_raises_value_error():
    """validate_symbols should raise ValueError when the list is empty."""
    with pytest.raises(ValueError, match="symbols must not be empty"):
        validate_symbols([], "fact_stock_bars")


def test_build_bars_params_returns_expected_dictionary():
    """build_bars_params should build the expected query parameters."""
    result = build_bars_params(["AAPL", "MSFT"], START_DATE, END_DATE)

    expected = {
        "symbols": "AAPL,MSFT",
        "timeframe": "1Day",
        "start": START_DATE,
        "end": END_DATE,
        "limit": 10000,
        "adjustment": "raw",
        "feed": "iex",
    }

    assert result == expected


def test_build_snapshot_params_returns_expected_dictionary():
    """build_snapshot_params should build the expected query parameters."""
    result = build_snapshot_params(["AAPL", "NVDA"])

    expected = {
        "symbols": "AAPL,NVDA",
        "feed": "iex",
    }

    assert result == expected


# Row parsers
def test_parse_bar_row_returns_expected_row(aapl_raw_bar):
    """parse_bar_row should map Alpaca bar fields into the expected output format."""
    result = parse_bar_row("AAPL", aapl_raw_bar, INGESTION_TIMESTAMP)

    expected = {
        "symbol": "AAPL",
        "bar_timestamp": "2026-03-24T00:00:00Z",
        "bar_date": "2026-03-24",
        "open": 210.0,
        "high": 215.0,
        "low": 208.0,
        "close": 214.0,
        "volume": 1000000,
        "trade_count": 25000,
        "vwap": 212.5,
        "ingestion_time": "2026-03-25T12:00:00",
    }

    assert result == expected


def test_parse_snapshot_row_returns_expected_row(aapl_raw_snapshot):
    """parse_snapshot_row should map Alpaca snapshot fields into the expected output format."""
    result = parse_snapshot_row("MSFT", aapl_raw_snapshot, INGESTION_TIMESTAMP)

    expected = {
        "symbol": "MSFT",
        "snapshot_time": "2026-03-25T11:30:00",
        "latest_trade_price": 220.5,
        "previous_close": 217.8,
        "current_day_open": 218.0,
        "current_day_high": 221.0,
        "current_day_low": 217.5,
        "current_day_volume": 500000,
        "current_day_vwap": 219.4,
        "current_day_trade_count": 14000,
        "ingestion_time": "2026-03-25T12:00:00",
    }

    assert result == expected


# Response helpers


def test_extract_bar_rows_from_response_returns_all_rows(aapl_raw_bar, msft_raw_bar):
    """extract_bar_rows_from_response should flatten all bars across all symbols."""
    data = {
        "bars": {
            "AAPL": [aapl_raw_bar],
            "MSFT": [msft_raw_bar],
        }
    }

    result = extract_bar_rows_from_response(data, INGESTION_TIMESTAMP)

    assert len(result) == 2
    assert result[0]["symbol"] == "AAPL"
    assert result[1]["symbol"] == "MSFT"


def test_rows_to_dataframe_returns_dataframe():
    """rows_to_dataframe should convert row dictionaries into a pandas DataFrame."""
    rows = [
        {"symbol": "AAPL", "close": 214.0},
        {"symbol": "MSFT", "close": 304.0},
    ]

    result = rows_to_dataframe(rows, "fact_stock_bars")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["symbol", "close"]


# HTTP layer for mocking

@patch("alpaca_extract.requests.get")
@patch("alpaca_extract.get_request_headers")
def test_make_request_returns_json_data(mock_get_request_headers, mock_get):
    """make_request should return JSON data when the request succeeds."""
    mock_get_request_headers.return_value = {"APCA-API-KEY-ID": "key"}

    mock_response = Mock()
    mock_response.json.return_value = {"bars": {}}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = make_request(
        "https://example.com",
        {"ignored": "header"},
        {"symbols": "AAPL"},
    )

    assert result == {"bars": {}}
    mock_get.assert_called_once()


@patch("alpaca_extract.requests.get")
@patch("alpaca_extract.get_request_headers")
def test_make_request_raises_request_exception_on_http_error(mock_get_request_headers, mock_get):
    """make_request should re-raise request errors."""
    mock_get_request_headers.return_value = {"APCA-API-KEY-ID": "key"}

    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError(
        "Bad request")
    mock_get.return_value = mock_response

    with pytest.raises(requests.RequestException):
        make_request(
            "https://example.com",
            {"ignored": "header"},
            {"symbols": "AAPL"},
        )


# Tests: extraction workflows


@patch("alpaca_extract.get_ingestion_time")
@patch("alpaca_extract.get_request_headers")
@patch("alpaca_extract.make_request")
def test_extract_fact_daily_stock_bars_single_page(
    mock_make_request,
    mock_get_request_headers,
    mock_get_ingestion_time,
    aapl_raw_bar,
):
    """extract_fact_daily_stock_bars should return parsed rows for a single response page."""
    mock_get_request_headers.return_value = {"header": "value"}
    mock_get_ingestion_time.return_value = INGESTION_TIMESTAMP

    mock_make_request.return_value = {
        "bars": {"AAPL": [aapl_raw_bar]},
        "next_page_token": None,
    }

    result = extract_fact_daily_stock_bars(["AAPL"], START_DATE, END_DATE)

    assert len(result) == 1
    assert result[0]["symbol"] == "AAPL"
    assert result[0]["close"] == 214.0


@patch("alpaca_extract.get_ingestion_time")
@patch("alpaca_extract.get_request_headers")
@patch("alpaca_extract.make_request")
def test_extract_fact_daily_stock_bars_multiple_pages(
    mock_make_request,
    mock_get_request_headers,
    mock_get_ingestion_time,
    aapl_raw_bar,
    aapl_raw_bar_prev_day,
):
    """extract_fact_daily_stock_bars should keep requesting until next_page_token is None."""
    mock_get_request_headers.return_value = {"header": "value"}
    mock_get_ingestion_time.return_value = INGESTION_TIMESTAMP

    mock_make_request.side_effect = [
        {
            "bars": {"AAPL": [aapl_raw_bar_prev_day]},
            "next_page_token": "page2",
        },
        {
            "bars": {"AAPL": [aapl_raw_bar]},
            "next_page_token": None,
        },
    ]

    result = extract_fact_daily_stock_bars(["AAPL"], START_DATE, END_DATE)

    assert len(result) == 2
    assert mock_make_request.call_count == 2
    assert result[0]["bar_timestamp"] == "2026-03-23T00:00:00Z"
    assert result[1]["bar_timestamp"] == "2026-03-24T00:00:00Z"


@patch("alpaca_extract.get_ingestion_time")
@patch("alpaca_extract.get_request_headers")
@patch("alpaca_extract.make_request")
def test_extract_fact_stock_snapshot_returns_rows(
    mock_make_request,
    mock_get_request_headers,
    mock_get_ingestion_time,
    aapl_raw_snapshot,
    msft_raw_snapshot,
):
    """extract_fact_stock_snapshot should return one row per symbol."""
    mock_get_request_headers.return_value = {"header": "value"}
    mock_get_ingestion_time.return_value = INGESTION_TIMESTAMP

    mock_make_request.return_value = {
        "AAPL": aapl_raw_snapshot,
        "MSFT": msft_raw_snapshot,
    }

    result = extract_fact_stock_snapshot(["AAPL", "MSFT"])

    assert len(result) == 2
    assert result[0]["symbol"] in ["AAPL", "MSFT"]
    assert result[1]["symbol"] in ["AAPL", "MSFT"]


@patch("alpaca_extract.extract_fact_daily_stock_bars")
@patch("alpaca_extract.extract_fact_stock_snapshot")
def test_extract_all_stock_data_returns_rag_dict_and_dataframes(
    mock_extract_fact_stock_snapshot,
    mock_extract_fact_daily_stock_bars,
):
    """extract_all_stock_data should return raw dictionaries and pandas DataFrames."""
    mock_extract_fact_daily_stock_bars.return_value = [
        {"symbol": "AAPL", "close": 214.0}
    ]
    mock_extract_fact_stock_snapshot.return_value = [
        {"symbol": "AAPL", "latest_trade_price": 220.5}
    ]

    result = extract_all_stock_data(["AAPL"], START_DATE, END_DATE)

    assert "rag_dict" in result
    assert "dataframes" in result
    assert "fact_stock_bars" in result["rag_dict"]
    assert "fact_stock_snapshot" in result["rag_dict"]

    assert isinstance(result["dataframes"]["fact_stock_bars"], pd.DataFrame)
    assert isinstance(result["dataframes"]
                      ["fact_stock_snapshot"], pd.DataFrame)

    assert result["rag_dict"]["fact_stock_bars"][0]["symbol"] == "AAPL"
    assert result["dataframes"]["fact_stock_snapshot"].iloc[0]["symbol"] == "AAPL"
