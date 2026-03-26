"""Test suite for RSS extraction module."""

import pytest
from unittest.mock import patch, MagicMock
from types import SimpleNamespace
import pandas as pd

with patch("rss_extract.OpenAI"), patch("rss_extract.logger"):
    from rss_extract import (
        fetch_feed,
        extract_entry_fields,
        format_ticker_prompt,
        get_relevant_tickers,
        create_dataframe,
    )


# === Fixtures ===

@pytest.fixture
def sample_entry():
    """Feedparser entry with all expected fields."""
    entry = SimpleNamespace(
        title="Apple unveils new chip",
        link="https://techcrunch.com/apple-chip",
        summary="Apple announced a new silicon chip today.",
        published="Mon, 24 Mar 2026 10:00:00 +0000",
    )
    entry.published_parsed = (2026, 3, 24, 10, 0, 0, 0, 0, 0)
    return entry


@pytest.fixture
def partial_entry():
    """Feedparser entry with missing optional fields."""
    return SimpleNamespace(title="Partial Item", link="https://example.com")


@pytest.fixture
def mock_feed(sample_entry):
    """Feedparser feed with one entry."""
    feed = MagicMock()
    feed.bozo = False
    feed.bozo_exception = None
    feed.entries = [sample_entry]
    return feed


@pytest.fixture
def sample_article():
    """Pre-extracted article dict."""
    return {
        "title": "Apple unveils new chip",
        "link": "https://techcrunch.com/apple-chip",
        "summary": "Apple announced a new silicon chip today.",
        "published_date": "2026-03-24 10:00:00",
        "source": "techcrunch",
    }


# === fetch_feed ===

class TestFetchFeed:
    """Tests for fetch_feed()."""

    def test_successful_fetch_returns_feed(self, mock_feed):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.content = b"<rss/>"
        with patch("rss_extract.requests.get", return_value=mock_response), \
                patch("rss_extract.feedparser.parse", return_value=mock_feed):
            result = fetch_feed("https://techcrunch.com/feed/")
        assert result is mock_feed

    def test_network_error_returns_none(self):
        import requests
        with patch("rss_extract.requests.get", side_effect=requests.RequestException("fail")):
            result = fetch_feed("https://techcrunch.com/feed/")
        assert result is None

    def test_empty_feed_returns_none(self):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.content = b"<rss/>"
        empty_feed = MagicMock()
        empty_feed.bozo = False
        empty_feed.entries = []
        with patch("rss_extract.requests.get", return_value=mock_response), \
                patch("rss_extract.feedparser.parse", return_value=empty_feed):
            result = fetch_feed("https://techcrunch.com/feed/")
        assert result is None


# === format_ticker_prompt ===


class TestFormatTickerPrompt:
    """Tests for format_ticker_prompt()."""

    def test_prompt_contains_ticker_and_company(self, sample_article):
        prompt = format_ticker_prompt(sample_article, ["AAPL"])
        assert "AAPL" in prompt
        assert "Apple" in prompt

    def test_prompt_contains_article_title(self, sample_article):
        prompt = format_ticker_prompt(sample_article, ["AAPL"])
        assert sample_article["title"] in prompt


# === get_relevant_tickers ===

class TestGetRelevantTickers:
    """Tests for get_relevant_tickers()."""

    def make_openai_response(self, content: str) -> MagicMock:
        msg = MagicMock()
        msg.content = content
        choice = MagicMock()
        choice.message = msg
        response = MagicMock()
        response.choices = [choice]
        return response

    def test_returns_matched_tickers(self, sample_article):
        with patch("rss_extract.CLIENT") as mock_client:
            mock_client.chat.completions.create.return_value = self.make_openai_response(
                "AAPL")
            result = get_relevant_tickers(sample_article, ["AAPL", "MSFT"])
        assert result == ["AAPL"]

    def test_returns_empty_on_none_response(self, sample_article):
        with patch("rss_extract.CLIENT") as mock_client:
            mock_client.chat.completions.create.return_value = self.make_openai_response(
                "NONE")
            result = get_relevant_tickers(sample_article, ["AAPL", "MSFT"])
        assert result == []

    def test_returns_multiple_tickers(self, sample_article):
        with patch("rss_extract.CLIENT") as mock_client:
            mock_client.chat.completions.create.return_value = self.make_openai_response(
                "AAPL, MSFT")
            result = get_relevant_tickers(sample_article, ["AAPL", "MSFT"])
        assert result == ["AAPL", "MSFT"]


# === create_dataframe ===

class TestCreateDataframe:
    """Tests for create_dataframe()."""

    def test_empty_list_returns_empty_dataframe(self):
        result = create_dataframe([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_articles_produce_correct_columns(self):
        articles = [{
            "ticker": "AAPL",
            "title": "Apple news",
            "link": "https://example.com",
            "summary": "Summary here.",
            "published_date": "2026-03-24 10:00:00",
            "source": "techcrunch",
        }]
        result = create_dataframe(articles)
        expected_cols = ["ticker", "title", "link",
                         "summary", "published_date", "source"]
        assert list(result.columns) == expected_cols

    def test_published_date_is_datetime(self):
        articles = [{
            "ticker": "AAPL",
            "title": "Apple news",
            "link": "https://example.com",
            "summary": "Summary.",
            "published_date": "2026-03-24 10:00:00",
            "source": "techcrunch",
        }]
        result = create_dataframe(articles)
        assert pd.api.types.is_datetime64_any_dtype(result["published_date"])
