"""Unit tests for the Alpaca RDS load module.

All database interactions are tested against a real in-memory-style
temporary PostgreSQL schema created inside a disposable transaction,
OR by mocking psycopg2 cursors — so no live RDS is required.
"""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from alpaca_load import (
    fetch_stock_id_map,
    map_ticker_to_stock_id,
    filter_new_history_rows,
    fetch_existing_history_range,
    insert_history_rows,
    insert_live_rows,
    get_live_window_start,
    delete_stale_live_rows,
    fetch_existing_live_keys,
    filter_new_live_rows,
    load_alpaca_history,
    load_alpaca_live,
    load_all_to_rds,
    HISTORY_COLUMNS,
    LIVE_COLUMNS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def stock_id_map():
    """A small ticker -> stock_id mapping like the stock table would give."""
    return {"AAPL": 1, "MSFT": 2, "NVDA": 3}


@pytest.fixture
def cleaned_history_df():
    """Cleaned history dataframe with ticker column (pre-mapping)."""
    return pd.DataFrame({
        "ticker": ["AAPL", "AAPL", "MSFT", "MSFT"],
        "bar_date": pd.to_datetime([
            "2026-03-24", "2026-03-25", "2026-03-24", "2026-03-25"
        ]),
        "open": [170.0, 171.0, 310.0, 312.0],
        "high": [175.0, 176.0, 315.0, 317.0],
        "low": [169.0, 170.0, 309.0, 311.0],
        "close": [174.0, 175.0, 314.0, 316.0],
        "volume": [1000000.0, 1100000.0, 900000.0, 950000.0],
        "trade_count": [5000.0, 5200.0, 4500.0, 4700.0],
        "vwap": [172.0, 173.0, 312.0, 314.0],
    })


@pytest.fixture
def cleaned_live_df():
    """Cleaned live dataframe with ticker column (pre-mapping)."""
    return pd.DataFrame({
        "ticker": ["AAPL", "MSFT"],
        "latest_time": pd.to_datetime([
            "2026-03-26T14:30:00Z", "2026-03-26T14:30:00Z"
        ]),
        "open": [175.0, 316.0],
        "high": [178.0, 319.0],
        "low": [174.0, 315.0],
        "close": [177.0, 318.0],
        "volume": [500000.0, 450000.0],
        "trade_count": [2500.0, 2300.0],
        "vwap": [176.0, 317.0],
    })


# ---------------------------------------------------------------------------
# fetch_stock_id_map
# ---------------------------------------------------------------------------

def test_fetch_stock_id_map_returns_dict():
    """Return a dict mapping ticker -> stock_id from mock cursor rows."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = [
        (1, "AAPL"), (2, "MSFT"), (3, "NVDA")
    ]

    result = fetch_stock_id_map(mock_conn)

    assert result == {"AAPL": 1, "MSFT": 2, "NVDA": 3}


def test_fetch_stock_id_map_raises_when_table_is_empty():
    """Raise ValueError when the stock table has no rows."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = []

    with pytest.raises(ValueError, match="stock table is empty"):
        fetch_stock_id_map(mock_conn)


# ---------------------------------------------------------------------------
# map_ticker_to_stock_id
# ---------------------------------------------------------------------------

def test_map_ticker_to_stock_id_replaces_ticker_with_stock_id(
        cleaned_history_df, stock_id_map):
    """Replace ticker column with stock_id and drop ticker."""
    result = map_ticker_to_stock_id(cleaned_history_df, stock_id_map)

    assert "stock_id" in result.columns
    assert "ticker" not in result.columns
    assert set(result["stock_id"].tolist()) == {1, 2}


def test_map_ticker_to_stock_id_drops_unmapped_tickers(stock_id_map):
    """Drop rows for tickers not present in the stock table."""
    df = pd.DataFrame({
        "ticker": ["AAPL", "UNKNOWN"],
        "bar_date": pd.to_datetime(["2026-03-24", "2026-03-24"]),
        "close": [174.0, 100.0],
    })

    result = map_ticker_to_stock_id(df, stock_id_map)

    assert len(result) == 1
    assert result.iloc[0]["stock_id"] == 1


# ---------------------------------------------------------------------------
# filter_new_history_rows
# ---------------------------------------------------------------------------

def test_filter_new_history_rows_returns_all_when_no_existing_data():
    """Return all rows when the RDS has no existing history."""
    df = pd.DataFrame({
        "stock_id": [1, 1, 2],
        "bar_date": pd.to_datetime(
            ["2026-03-24", "2026-03-25", "2026-03-24"]),
        "close": [174.0, 175.0, 314.0],
    })

    result = filter_new_history_rows(df, date_ranges={})

    assert len(result) == 3


def test_filter_new_history_rows_keeps_only_newer_rows():
    """Keep only rows with bar_date after the latest existing date."""
    df = pd.DataFrame({
        "stock_id": [1, 1, 1],
        "bar_date": pd.to_datetime(
            ["2026-03-23", "2026-03-24", "2026-03-25"]),
        "close": [173.0, 174.0, 175.0],
    })

    date_ranges = {1: (date(2026, 3, 23), date(2026, 3, 24))}

    result = filter_new_history_rows(df, date_ranges)

    assert len(result) == 1
    assert pd.to_datetime(result.iloc[0]["bar_date"]).date() == date(
        2026, 3, 25)


def test_filter_new_history_rows_handles_mixed_stocks():
    """Filter independently per stock_id."""
    df = pd.DataFrame({
        "stock_id": [1, 1, 2, 2],
        "bar_date": pd.to_datetime([
            "2026-03-24", "2026-03-25", "2026-03-24", "2026-03-25"
        ]),
        "close": [174.0, 175.0, 314.0, 316.0],
    })

    date_ranges = {
        1: (date(2026, 3, 24), date(2026, 3, 24)),
        2: (date(2026, 3, 24), date(2026, 3, 25)),
    }

    result = filter_new_history_rows(df, date_ranges)

    # stock 1: 03-25 is new (after max). stock 2: fully covered.
    assert len(result) == 1
    assert result.iloc[0]["stock_id"] == 1


def test_filter_new_history_rows_includes_new_stock_not_in_rds():
    """A stock_id that has no RDS history at all keeps all its rows."""
    df = pd.DataFrame({
        "stock_id": [1, 3, 3],
        "bar_date": pd.to_datetime(
            ["2026-03-25", "2026-03-24", "2026-03-25"]),
        "close": [175.0, 200.0, 201.0],
    })

    date_ranges = {1: (date(2026, 3, 25), date(2026, 3, 25))}

    result = filter_new_history_rows(df, date_ranges)

    # stock 1: nothing new. stock 3: both rows new (not in date_ranges).
    assert len(result) == 2
    assert set(result["stock_id"].tolist()) == {3}


def test_filter_new_history_rows_allows_backfill():
    """Rows older than the existing min date are kept for backfill."""
    df = pd.DataFrame({
        "stock_id": [1, 1, 1, 1],
        "bar_date": pd.to_datetime([
            "2024-06-01", "2024-06-02", "2026-03-25", "2026-03-26"
        ]),
        "close": [150.0, 151.0, 175.0, 176.0],
    })

    # RDS already has 2026-01-01 to 2026-03-25
    date_ranges = {1: (date(2026, 1, 1), date(2026, 3, 25))}

    result = filter_new_history_rows(df, date_ranges)

    # 2024-06-01 and 2024-06-02 are before min (backfill)
    # 2026-03-26 is after max (incremental)
    # 2026-03-25 is within range (skip)
    assert len(result) == 3
    expected_dates = {date(2024, 6, 1), date(2024, 6, 2), date(2026, 3, 26)}
    actual_dates = set(pd.to_datetime(result["bar_date"]).dt.date)
    assert actual_dates == expected_dates


# ---------------------------------------------------------------------------
# insert_history_rows
# ---------------------------------------------------------------------------

def test_insert_history_rows_returns_zero_for_empty_df():
    """Return 0 when there are no rows to insert."""
    mock_conn = MagicMock()

    result = insert_history_rows(mock_conn, pd.DataFrame())

    assert result == 0


def test_insert_history_rows_calls_execute_values():
    """Verify execute_values is called with the correct number of tuples."""
    df = pd.DataFrame({
        "stock_id": [1, 2],
        "bar_date": pd.to_datetime(["2026-03-25", "2026-03-25"]),
        "open": [170.0, 310.0],
        "high": [175.0, 315.0],
        "low": [169.0, 309.0],
        "close": [174.0, 314.0],
        "volume": [1000000.0, 900000.0],
        "trade_count": [5000.0, 4500.0],
        "vwap": [172.0, 312.0],
    })

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("alpaca_load.execute_values") as mock_exec:
        result = insert_history_rows(mock_conn, df)

    assert result == 2
    mock_exec.assert_called_once()
    # The third arg to execute_values is the list of tuples
    inserted_tuples = mock_exec.call_args[0][2]
    assert len(inserted_tuples) == 2


# ---------------------------------------------------------------------------
# get_live_window_start
# ---------------------------------------------------------------------------

def test_get_live_window_start_returns_todays_noon_when_after_noon():
    """After 12:00 UK the window starts at today's noon."""
    # Simulate 27 Mar 2026 14:00 UK time (BST = UTC+1 in March)
    fake_now = datetime(2026, 3, 27, 14, 0, 0,
                        tzinfo=ZoneInfo("Europe/London"))

    with patch("alpaca_load.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = get_live_window_start()

    expected_uk_noon = datetime(
        2026, 3, 27, 12, 0, 0, tzinfo=ZoneInfo("Europe/London")
    ).astimezone(timezone.utc)

    assert result == expected_uk_noon


def test_get_live_window_start_returns_yesterdays_noon_when_before_noon():
    """Before 12:00 UK the window starts at yesterday's noon."""
    # Simulate 27 Mar 2026 09:00 UK time
    fake_now = datetime(2026, 3, 27, 9, 0, 0, tzinfo=ZoneInfo("Europe/London"))

    with patch("alpaca_load.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = get_live_window_start()

    expected_uk_noon = datetime(
        2026, 3, 26, 12, 0, 0, tzinfo=ZoneInfo("Europe/London")
    ).astimezone(timezone.utc)

    assert result == expected_uk_noon


# ---------------------------------------------------------------------------
# delete_stale_live_rows
# ---------------------------------------------------------------------------

def test_delete_stale_live_rows_returns_rowcount():
    """Return the number of rows deleted from the mock cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.rowcount = 5

    result = delete_stale_live_rows(mock_conn)

    assert result == 5
    mock_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# filter_new_live_rows
# ---------------------------------------------------------------------------

def test_filter_new_live_rows_skips_existing_rows():
    """Skip rows whose (stock_id, latest_time) already exist in the RDS."""
    ts = pd.Timestamp("2026-03-27T14:30:00Z")
    df = pd.DataFrame({
        "stock_id": [1, 2],
        "latest_time": [ts, ts],
        "close": [177.0, 318.0],
    })

    existing_keys = {(1, ts)}

    result = filter_new_live_rows(df, existing_keys)

    assert len(result) == 1
    assert result.iloc[0]["stock_id"] == 2


def test_filter_new_live_rows_returns_all_when_no_existing():
    """Return all rows when the existing key set is empty."""
    ts = pd.Timestamp("2026-03-27T14:30:00Z")
    df = pd.DataFrame({
        "stock_id": [1, 2],
        "latest_time": [ts, ts],
        "close": [177.0, 318.0],
    })

    result = filter_new_live_rows(df, existing_keys=set())

    assert len(result) == 2


# ---------------------------------------------------------------------------
# insert_live_rows
# ---------------------------------------------------------------------------

def test_insert_live_rows_returns_zero_for_empty_df():
    """Return 0 when there are no live rows to insert."""
    mock_conn = MagicMock()

    result = insert_live_rows(mock_conn, pd.DataFrame())

    assert result == 0


def test_insert_live_rows_calls_execute_values():
    """Verify execute_values is called with correct live row tuples."""
    df = pd.DataFrame({
        "stock_id": [1, 2],
        "latest_time": pd.to_datetime([
            "2026-03-26T14:30:00Z", "2026-03-26T14:30:00Z"
        ]),
        "open": [175.0, 316.0],
        "high": [178.0, 319.0],
        "low": [174.0, 315.0],
        "close": [177.0, 318.0],
        "volume": [500000.0, 450000.0],
        "trade_count": [2500.0, 2300.0],
        "vwap": [176.0, 317.0],
    })

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("alpaca_load.execute_values") as mock_exec:
        result = insert_live_rows(mock_conn, df)

    assert result == 2
    mock_exec.assert_called_once()
    inserted_tuples = mock_exec.call_args[0][2]
    assert len(inserted_tuples) == 2


# ---------------------------------------------------------------------------
# load_alpaca_history (integration with mocks)
# ---------------------------------------------------------------------------

def test_load_alpaca_history_inserts_only_new_rows(
        cleaned_history_df, stock_id_map):
    """On a second run, only rows newer than the existing max date are inserted."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Simulate: stock 1 (AAPL) already has data from 2026-03-24 to 2026-03-24,
    #           stock 2 (MSFT) already has data from 2026-03-24 to 2026-03-25
    mock_cursor.fetchall.return_value = [
        (1, date(2026, 3, 24), date(2026, 3, 24)),
        (2, date(2026, 3, 24), date(2026, 3, 25)),
    ]

    with patch("alpaca_load.execute_values") as mock_exec:
        result = load_alpaca_history(
            mock_conn, cleaned_history_df, stock_id_map)

    # Only AAPL 2026-03-25 should be new (1 row)
    assert result == 1
    inserted_tuples = mock_exec.call_args[0][2]
    assert len(inserted_tuples) == 1


def test_load_alpaca_history_loads_all_on_first_run(
        cleaned_history_df, stock_id_map):
    """On first run with empty RDS, all history rows are inserted."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Simulate empty alpaca_history table
    mock_cursor.fetchall.return_value = []

    with patch("alpaca_load.execute_values") as mock_exec:
        result = load_alpaca_history(
            mock_conn, cleaned_history_df, stock_id_map)

    # All 4 rows should be inserted
    assert result == 4
    inserted_tuples = mock_exec.call_args[0][2]
    assert len(inserted_tuples) == 4


# ---------------------------------------------------------------------------
# load_alpaca_live (integration with mocks)
# ---------------------------------------------------------------------------

def test_load_alpaca_live_deletes_stale_then_inserts_new(
        cleaned_live_df, stock_id_map):
    """Delete stale rows, skip existing, insert new."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.rowcount = 0  # no stale rows to delete

    # fetch_existing_live_keys returns empty set (first run today)
    mock_cursor.fetchall.return_value = []

    with patch("alpaca_load.execute_values") as mock_exec:
        result = load_alpaca_live(
            mock_conn, cleaned_live_df, stock_id_map)

    assert result == 2


def test_load_alpaca_live_skips_duplicates_on_rerun(
        cleaned_live_df, stock_id_map):
    """On a second run the same day, no rows are inserted."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.rowcount = 0

    # Both rows already exist from the first run
    ts = pd.Timestamp("2026-03-26T14:30:00+00:00")
    mock_cursor.fetchall.return_value = [(1, ts), (2, ts)]

    with patch("alpaca_load.execute_values") as mock_exec:
        result = load_alpaca_live(
            mock_conn, cleaned_live_df, stock_id_map)

    assert result == 0
    mock_exec.assert_not_called()


# ---------------------------------------------------------------------------
# load_all_to_rds (orchestrator)
# ---------------------------------------------------------------------------

def test_load_all_to_rds_calls_both_loaders(
        cleaned_history_df, cleaned_live_df, stock_id_map):
    """Orchestrator calls both history and live loaders."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(
        return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.side_effect = [
        [(1, "AAPL"), (2, "MSFT"), (3, "NVDA")],  # stock table
        [],                                         # empty history (first run)
        # existing live keys (empty)
        [],
    ]
    mock_cursor.rowcount = 0

    cleaned_data = {
        "alpaca_history": cleaned_history_df,
        "alpaca_live": cleaned_live_df,
    }

    with patch("alpaca_load.execute_values"):
        result = load_all_to_rds(cleaned_data, connection=mock_conn)

    assert "history_rows_inserted" in result
    assert "live_rows_inserted" in result
    assert result["history_rows_inserted"] == 4
    assert result["live_rows_inserted"] == 2
