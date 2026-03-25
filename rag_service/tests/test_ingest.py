from app.ingest import convert_to_documents
import pytest


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


def test_convert_to_documents(sample_nvda_data):
    """Test the convert_to_documents function with sample data."""

    docs = convert_to_documents(sample_nvda_data)

    assert len(docs) == 1
    assert docs[0]["metadata"]["ticker"] == "NVDA"
    assert "close 124.7" in docs[0]["text"]


def test_convert_to_documents_multiple_records(sample_apple_data, sample_nvda_data, sample_msft_data):
    """Test the convert_to_documents function with multiple records."""

    combined_data = sample_apple_data + sample_nvda_data + sample_msft_data
    docs = convert_to_documents(combined_data)

    assert len(docs) == 3
    tickers = {doc["metadata"]["ticker"] for doc in docs}
    assert tickers == {"AAPL", "NVDA", "MSFT"}
    assert any("close 154.0" in doc["text"]
               for doc in docs if doc["metadata"]["ticker"] == "AAPL")
    assert any("close 124.7" in doc["text"]
               for doc in docs if doc["metadata"]["ticker"] == "NVDA")
    assert any("close 254.0" in doc["text"]
               for doc in docs if doc["metadata"]["ticker"] == "MSFT")


def test_convert_to_documents_empty_data():
    """Test the convert_to_documents function with empty data."""

    docs = convert_to_documents([])

    assert len(docs) == 0


def test_convert_to_documents_missing_fields(sample_incomplete_data):
    """Test the convert_to_documents function with incomplete data."""

    docs = convert_to_documents(sample_incomplete_data)

    assert len(docs) == 1
    assert docs[0]["metadata"]["ticker"] == "AAPL"
    assert "open 150.0" in docs[0]["text"]
    assert "high" not in docs[0]["text"]
    assert "low" not in docs[0]["text"]
    assert "close" not in docs[0]["text"]
    assert "volume" not in docs[0]["text"]


def test_convert_to_documents_no_required_metrics(sample_no_metrics_data):
    """Test the convert_to_documents function with data that has no required metrics."""

    docs = convert_to_documents(sample_no_metrics_data)

    assert len(docs) == 0


def test_convert_to_documents_missing_ticker(sample_missing_ticker_data):
    """Test the convert_to_documents function with data missing the ticker field."""

    docs = convert_to_documents(sample_missing_ticker_data)

    assert len(docs) == 0


def test_convert_to_documents_missing_timestamp(sample_missing_timestamp_data):
    """Test the convert_to_documents function with data missing the timestamp field."""

    docs = convert_to_documents(sample_missing_timestamp_data)

    assert len(docs) == 0
