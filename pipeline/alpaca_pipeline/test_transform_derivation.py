import pandas as pd
import pytest

from alpaca_transform import (
    add_return_metrics,
    add_moving_average_metrics,
    add_volatility_metrics,
    add_drawdown_metrics,
    add_relative_performance_metrics,
    add_volume_signal_metrics,
    filter_dashboard_ready_rows
)


def test_add_return_metrics_calculates_1m_and_3m_correctly():
    """Test that 1 month and 3 month returns are calculated correctly."""
    # Arrange
    close_prices = []

    for number in range(1, 65):
        close_prices.append(number)

    df = pd.DataFrame({
        "symbol": ["AAPL"] * 64,
        "close": close_prices
    })

    # Act
    result = add_return_metrics(df)

    # Assert
    expected_return_1m = round((64 / 43) - 1, 4)
    expected_return_3m = round((64 / 1) - 1, 4)

    assert result.loc[63, "return_1m"] == expected_return_1m
    assert result.loc[63, "return_3m"] == expected_return_3m


def test_add_moving_average_metrics_calculates_ma_50_and_price_vs_ma_50_correctly():
    """Test that 50 day moving average and price vs MA are calculated correctly."""
    # Arrange
    close_prices = []

    for number in range(1, 51):
        close_prices.append(float(number))

    df = pd.DataFrame({
        "symbol": ["AAPL"] * 50,
        "close": close_prices
    })

    # Act
    result = add_moving_average_metrics(df)

    # Assert
    expected_ma_50 = 25.5
    expected_price_vs_ma_50 = round(50 / 25.5, 4)

    assert result.loc[49, "ma_50"] == expected_ma_50
    assert result.loc[49, "price_vs_ma_50"] == expected_price_vs_ma_50


def test_add_drawdown_metrics_calculates_drawdown_correctly():
    """Test that drawdown is calculated from the running peak correctly."""
    # Arrange
    df = pd.DataFrame({
        "symbol": ["AAPL", "AAPL", "AAPL"],
        "close": [100.0, 120.0, 90.0]
    })

    # Act
    result = add_drawdown_metrics(df)

    # Assert
    expected_drawdown = round((90.0 - 120.0) / 120.0, 4)

    assert result.loc[0, "drawdown"] == 0.0
    assert result.loc[1, "drawdown"] == 0.0
    assert result.loc[2, "drawdown"] == expected_drawdown


def test_add_relative_performance_metrics_calculates_relative_return_1m_correctly():
    """Test that relative 1 month return is calculated against the market average correctly."""
    # Arrange
    df = pd.DataFrame({
        "symbol": ["AAPL", "MSFT"],
        "bar_date": ["2026-03-24", "2026-03-24"],
        "return_1m": [0.20, 0.10]
    })

    # Act
    result = add_relative_performance_metrics(df)

    # Assert
    expected_market_return_1m = 0.15
    expected_aapl_relative = 0.05
    expected_msft_relative = -0.05

    assert result.loc[0, "market_return_1m"] == expected_market_return_1m
    assert result.loc[1, "market_return_1m"] == expected_market_return_1m
    assert result.loc[0, "relative_return_1m"] == expected_aapl_relative
    assert result.loc[1, "relative_return_1m"] == expected_msft_relative


def test_add_volume_signal_metrics_calculates_relative_volume_20d_correctly():
    """Test that 20 day average volume and relative volume are calculated correctly."""
    # Arrange
    volumes = [100.0] * 19 + [200.0]

    df = pd.DataFrame({
        "symbol": ["AAPL"] * 20,
        "volume": volumes
    })

    # Act
    result = add_volume_signal_metrics(df)

    # Assert
    expected_avg_volume_20d = 105.0
    expected_relative_volume_20d = round(200.0 / 105.0, 4)

    assert result.loc[19, "avg_volume_20d"] == expected_avg_volume_20d
    assert result.loc[19,
                      "relative_volume_20d"] == expected_relative_volume_20d


def test_filter_dashboard_ready_rows_keeps_only_complete_rows():
    """Test that dashboard filtering keeps only rows with all required metrics present."""
    # Arrange
    df = pd.DataFrame({
        "return_1m": [0.10, None],
        "return_3m": [0.20, 0.30],
        "price_vs_ma_50": [1.05, 1.02],
        "volatility_20d": [0.25, 0.30],
        "drawdown": [-0.10, -0.05],
        "relative_return_1m": [0.03, 0.01],
        "relative_volume_20d": [1.50, 1.20]
    })

    # Act
    result = filter_dashboard_ready_rows(df)

    # Assert
    assert len(result) == 1


def test_add_volatility_metrics_returns_zero_for_constant_returns():
    """Test that volatility is zero when daily returns are constant."""
    # Arrange
    close_prices = []

    price = 100.0
    for i in range(25):
        close_prices.append(price)
        price = price * 1.01

    df = pd.DataFrame({
        "symbol": ["AAPL"] * 25,
        "close": close_prices
    })

    df["daily_return"] = df.groupby("symbol")["close"].pct_change()

    # Act
    result = add_volatility_metrics(df)

    # Assert
    assert round(result.loc[24, "volatility_20d"], 10) == 0.0
