import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def sample_apple_data():
    """Provide sample stock data for testing."""
    return [
        {
            "ticker": "AAPL",
            "timestamp": "2026-03-25T14:30:00Z",
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 154.0,
            "volume": 10000000
        }
    ]


@pytest.fixture
def sample_nvda_data():
    """Provide sample stock data for testing."""
    return [
        {
            "ticker": "NVDA",
            "timestamp": "2026-03-25T14:30:00Z",
            "open": 120.5,
            "high": 125.2,
            "low": 119.8,
            "close": 124.7,
            "volume": 12345678
        }
    ]


@pytest.fixture
def sample_msft_data():
    """Provide sample stock data for testing."""
    return [
        {
            "ticker": "MSFT",
            "timestamp": "2026-03-25T14:30:00Z",
            "open": 250.0,
            "high": 255.0,
            "low": 249.0,
            "close": 254.0,
            "volume": 8000000
        }
    ]


@pytest.fixture
def sample_empty_data():
    """Provide empty stock data for testing."""
    return []


@pytest.fixture
def sample_incomplete_data():
    """Provide incomplete stock data for testing."""
    return [
        {
            "ticker": "AAPL",
            "timestamp": "2026-03-25T14:30:00Z",
            "open": 150.0,
            # Missing high, low, close, volume
        }
    ]


@pytest.fixture
def sample_no_metrics_data():
    """Provide stock data with no required metrics for testing."""
    return [
        {
            "ticker": "AAPL",
            "timestamp": "2026-03-25T14:30:00Z",
            # No open, high, low, close, volume
        }
    ]


@pytest.fixture
def sample_missing_ticker_data():
    """Provide stock data missing the ticker field for testing."""
    return [
        {
            "timestamp": "2026-03-25T14:30:00Z",
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 154.0,
            "volume": 10000000
        }
    ]


@pytest.fixture
def sample_missing_timestamp_data():
    """Provide stock data missing the timestamp field for testing."""
    return [
        {
            "ticker": "AAPL",
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 154.0,
            "volume": 10000000
        }
    ]
