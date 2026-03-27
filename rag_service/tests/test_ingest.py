from app.ingest import normalise_alpaca_data


def test_convert_to_documents(sample_nvda_data):
    """Test the normalise_alpaca_data function with sample data."""

    docs = normalise_alpaca_data(sample_nvda_data)

    assert len(docs) == 1
    assert docs[0]["metadata"]["ticker"] == "NVDA"
    assert "close 124.7" in docs[0]["text"]


def test_convert_to_documents_multiple_records(sample_apple_data, sample_nvda_data, sample_msft_data):
    """Test the normalise_alpaca_data function with multiple records."""

    combined_data = sample_apple_data + sample_nvda_data + sample_msft_data
    docs = normalise_alpaca_data(combined_data)

    assert len(docs) == 3
    tickers = {doc["metadata"]["ticker"] for doc in docs}
    assert tickers == {"AAPL", "NVDA", "MSFT"}
    assert any("close 154.0" in doc["text"]
               for doc in docs if doc["metadata"]["ticker"] == "AAPL")
    assert any("close 124.7" in doc["text"]
               for doc in docs if doc["metadata"]["ticker"] == "NVDA")
    assert any("close 254.0" in doc["text"]
               for doc in docs if doc["metadata"]["ticker"] == "MSFT")


def test_normalise_alpaca_data_empty_data():
    """Test the normalise_alpaca_data function with empty data."""

    docs = normalise_alpaca_data([])

    assert len(docs) == 0


def test_normalise_alpaca_data_missing_fields(sample_incomplete_data):
    """Test the normalise_alpaca_data function with incomplete data."""

    docs = normalise_alpaca_data(sample_incomplete_data)

    assert len(docs) == 1
    assert docs[0]["metadata"]["ticker"] == "AAPL"
    assert "open 150.0" in docs[0]["text"]
    assert "high" not in docs[0]["text"]
    assert "low" not in docs[0]["text"]
    assert "close" not in docs[0]["text"]
    assert "volume" not in docs[0]["text"]


def test_normalise_alpaca_data_no_required_metrics(sample_no_metrics_data):
    """Test the normalise_alpaca_data function with data that has no required metrics."""

    docs = normalise_alpaca_data(sample_no_metrics_data)

    assert len(docs) == 0


def test_normalise_alpaca_data_missing_ticker(sample_missing_ticker_data):
    """Test the normalise_alpaca_data function with data missing the ticker field."""

    docs = normalise_alpaca_data(sample_missing_ticker_data)

    assert len(docs) == 0


def test_normalise_alpaca_data_missing_timestamp(sample_missing_timestamp_data):
    """Test the normalise_alpaca_data function with data missing the timestamp field."""

    docs = normalise_alpaca_data(sample_missing_timestamp_data)

    assert len(docs) == 0
