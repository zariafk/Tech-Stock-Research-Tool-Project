"""Test suite for RSS extraction module."""


import pytest
from unittest.mock import patch, MagicMock
from types import SimpleNamespace
import feedparser
import requests

from pipeline.rss.rss_extract_yahoo import fetch_rss_feed, extract_article_fields

# === Fixtures ===


@pytest.fixture
def sample_entry():
    """A SimpleNamespace that mimics a feedparser entry with all expected fields."""
    return SimpleNamespace(
        title="Apple Hits All-Time High",
        link="https://finance.yahoo.com/news/apple-hits-all-time-high",
        published="Mon, 01 Jan 2024 12:00:00 +0000",
        summary="Apple Inc. shares reached a record high on Monday...",
        source="Reuters",
    )


@pytest.fixture
def partial_entry():
    """A SimpleNamespace that mimics a feedparser entry with *missing* fields."""
    return SimpleNamespace(
        title="Partial News Item",
        link="https://finance.yahoo.com/news/partial",
        # 'published', 'summary', and 'source' are intentionally absent
    )


@pytest.fixture
def mock_feed_with_entries(sample_entry):
    """A MagicMock that looks like a feedparser feed object with two entries."""
    second_entry = SimpleNamespace(
        title="Microsoft Acquires Startup",
        link="https://finance.yahoo.com/news/msft-acquires",
        published="Tue, 02 Jan 2024 09:30:00 +0000",
        summary="Microsoft announced the acquisition of...",
        source="Bloomberg",
    )
    feed = MagicMock()
    feed.entries = [sample_entry, second_entry]
    return feed


@pytest.fixture
def empty_feed():
    """A MagicMock that looks like a feedparser feed with no entries."""
    feed = MagicMock()
    feed.entries = []
    return feed


@pytest.fixture
def sample_rss_xml():
    """Minimal valid RSS 2.0 XML string used as fake HTTP response body."""
    return """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Yahoo Finance - AAPL</title>
    <item>
      <title>Apple Hits All-Time High</title>
      <link>https://finance.yahoo.com/news/apple-hits-all-time-high</link>
      <pubDate>Mon, 01 Jan 2024 12:00:00 +0000</pubDate>
      <description>Apple Inc. shares reached a record high on Monday...</description>
    </item>
  </channel>
</rss>"""


# === Tests ===
class TestFetchRssFeed:
    """Tests for the fetch_rss_feed() function."""

    def test_successful_fetch_returns_text(self, sample_rss_xml):
        """fetch_rss_feed should return the response body when HTTP 200 is received."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_rss_xml

        with patch("requests.get", return_value=mock_response) as mock_get:
            result = fetch_rss_feed("https://fake-url.example.com/rss")

        # Verify the function called requests.get exactly once with the right URL
        mock_get.assert_called_once_with(
            "https://fake-url.example.com/rss", timeout=10
        )
        assert result == sample_rss_xml, "Should return the raw XML text on HTTP 200"
