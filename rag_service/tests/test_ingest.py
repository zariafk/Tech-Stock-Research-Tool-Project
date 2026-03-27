import json
import os
from app.ingest import convert_to_documents

BASE_DIR = os.path.dirname(os.path.dirname(__file__))


def load_json(filename):
    path = os.path.join(BASE_DIR, "data", filename)
    with open(path) as f:
        return json.load(f)


class TestRssIngest:
    def test_rss(self):
        """Test that RSS data is correctly converted to documents."""
        data = load_json("sample_rss.json")
        docs = convert_to_documents(data, "rss")
        assert len(docs) == 1
        assert "text" in docs[0]
        assert "metadata" in docs[0]

    def test_rss_minimal(self):
        """Test that minimal RSS data is still converted to a document."""
        data = [
            {
                "text": "Test text",
                "metadata": {"ticker": "AAPL"}
            }
        ]

        docs = convert_to_documents(data, "rss")
        assert len(docs) == 1


class TestRedditIngest:
    def test_reddit(self):
        """Test that Reddit data is correctly converted to documents."""
        data = load_json("sample_reddit.json")
        docs = convert_to_documents(data, "reddit")
        assert len(docs) == 2  # 2 tickers
        for doc in docs:
            assert "ticker" in doc["metadata"]

    def test_reddit_edge_cases(self):
        """Test that Reddit data with missing fields is handled gracefully."""
        data = [
            {
                "post_id": "test1",
                "title": "",
                "contents": "Only body text",
                "created_at": "2026-03-27T12:00:00Z",
                "tickers": [
                    {"ticker": "AAPL"},
                    {"ticker": "MSFT", "analysis": "Strong trend"}
                ]
            }
        ]

        docs = convert_to_documents(data, "reddit")
        assert len(docs) == 2

    def test_reddit_missing_ticker(self):
        """Test that Reddit data with missing ticker is handled gracefully."""
        data = [
            {
                "post_id": "test2",
                "title": "Test",
                "contents": "Test body",
                "created_at": "2026-03-27T12:00:00Z",
                "tickers": [{}]
            }
        ]

        docs = convert_to_documents(data, "reddit")
        assert len(docs) == 0


class TestAlpacaIngest:
    def test_alpaca_live(self):
        """Test that Alpaca live bar data is correctly converted to documents."""
        data = load_json("sample_alpaca_live.json")
        docs = convert_to_documents(data, "alpaca")
        assert len(docs) == 1
        assert docs[0]["metadata"]["doc_type"] == "live_bar"

    def test_alpaca_historical(self):
        """Test that Alpaca historical data is correctly converted to documents."""
        data = load_json("sample_alpaca_history.json")
        docs = convert_to_documents(data, "alpaca")
        assert len(docs) == 1
        assert docs[0]["metadata"]["doc_type"] == "daily_summary"

    def test_alpaca_partial_data(self):
        """Test that Alpaca data with missing fields is still converted to a document."""
        data = [
            {
                "ticker": "NVDA",
                "latest_time": "2026-03-27T13:20:00Z",
                "close": 900
            }
        ]

        docs = convert_to_documents(data, "alpaca")
        assert len(docs) == 1

    def test_alpaca_invalid(self):
        """Test that Alpaca data with invalid fields is not converted to a document."""
        data = [
            {
                "ticker": None
            }
        ]

        docs = convert_to_documents(data, "alpaca")
        assert len(docs) == 0


class TestMixedIngest:
    def test_mixed_batch(self):
        """Test that a mixed batch of data from all sources is correctly converted to documents."""
        rss = [{"text": "Test", "metadata": {"ticker": "AAPL"}}]

        reddit = [{
            "post_id": "x",
            "title": "Test",
            "contents": "Body",
            "created_at": "2026-03-27T12:00:00Z",
            "tickers": [{"ticker": "MSFT"}]
        }]

        alpaca = [{
            "ticker": "NVDA",
            "latest_time": "2026-03-27T13:20:00Z",
            "close": 900
        }]

        assert len(convert_to_documents(rss, "rss")) == 1
        assert len(convert_to_documents(reddit, "reddit")) == 1
        assert len(convert_to_documents(alpaca, "alpaca")) == 1
