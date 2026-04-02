"""Unit tests for RSS extraction: live and historical."""

import pytest
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime
import feedparser
import pandas as pd


# ===== Fixtures =====

@pytest.fixture
def mock_feed_entry():
    """Mock RSS feed entry."""
    entry = MagicMock(spec=feedparser.FeedParserDict)
    entry.title = "Apple announces new chip"
    entry.link = "https://techcrunch.com/apple-chip"
    entry.summary = "Apple unveiled M4 processor today."
    entry.published_parsed = (2026, 3, 26, 10, 0, 0, 0, 0, 0)
    entry.get = MagicMock(side_effect=lambda k, d='N/A': {
        'title': 'Apple announces new chip',
        'link': 'https://techcrunch.com/apple-chip',
        'summary': 'Apple unveiled M4 processor today.',
        'description': 'Apple unveiled M4 processor today.',
    }.get(k, d))
    return entry


@pytest.fixture
def mock_feed():
    """Mock parsed RSS feed."""
    entry = MagicMock(spec=feedparser.FeedParserDict)
    entry.title = "Tech news"
    entry.link = "https://example.com/news"
    entry.summary = "Latest tech updates"
    entry.published_parsed = (2026, 3, 26, 15, 30, 0, 0, 0, 0)

    feed = MagicMock(spec=feedparser.FeedParserDict)
    feed.entries = [entry]
    return feed


@pytest.fixture
def sample_rss_feed_dict():
    """Sample RSS feeds dictionary."""
    return {
        'techcrunch': 'https://techcrunch.com/feed/',
        'hackernews': 'https://hnrss.org/frontpage'
    }


@pytest.fixture
def sample_hn_hit():
    """Mock Hacker News Algolia hit."""
    return {
        'title': 'Apple releases new AI features',
        'url': 'https://apple.com/ai',
        'story_url': 'https://news.ycombinator.com/item?id=12345',
        'points': 250,
        'num_comments': 45,
        'created_at_i': 1711497600,
    }


@pytest.fixture
def sample_ticker_map():
    """Sample ticker to company name mapping."""
    return {
        'AAPL': 'Apple',
        'MSFT': 'Microsoft',
        'GOOGL': 'Google',
    }


# ===== LIVE EXTRACTION TESTS =====

class TestLiveExtraction:
    """Tests for live RSS extraction functions."""

    # ===== Tests: extract_entry_fields =====

    class TestExtractEntryFields:
        """Tests for extract_entry_fields()."""

        def test_extracts_all_fields(self, mock_feed_entry):
            """Should extract all RSS entry fields."""
            from rss_extract_live import extract_entry_fields

            result = extract_entry_fields(mock_feed_entry, "techcrunch")

            assert result["title"] == "Apple announces new chip"
            assert result["url"] == "https://techcrunch.com/apple-chip"
            assert result["summary"] == "Apple unveiled M4 processor today."
            assert result["source"] == "techcrunch"

        def test_formats_published_date(self, mock_feed_entry):
            """Should format published_date as YYYY-MM-DD HH:MM:SS."""
            from rss_extract_live import extract_entry_fields

            result = extract_entry_fields(mock_feed_entry, "techcrunch")

            assert result["published_date"] == "2026-03-26 10:00:00"

        def test_handles_missing_summary_falls_back_to_description(self):
            """Should fall back to description if summary missing."""
            from rss_extract_live import extract_entry_fields

            entry = MagicMock(spec=feedparser.FeedParserDict)
            entry.title = "News"
            entry.link = "https://example.com"
            entry.get = MagicMock(
                side_effect=lambda k, d: d if k == "summary" else "Description text")
            entry.published_parsed = (2026, 3, 26, 10, 0, 0, 0, 0, 0)

            result = extract_entry_fields(entry, "source")

            assert "Description text" in result["summary"]

        def test_handles_missing_published_date(self):
            """Should set N/A for missing published_date."""
            from rss_extract_live import extract_entry_fields

            entry = MagicMock(spec=feedparser.FeedParserDict)
            entry.title = "News"
            entry.link = "https://example.com"
            entry.summary = "Summary"
            entry.published_parsed = None

            result = extract_entry_fields(entry, "source")

            assert result["published_date"] == "N/A"

        def test_handles_missing_fields_with_defaults(self):
            """Should use N/A for completely missing fields."""
            from rss_extract_live import extract_entry_fields

            entry = MagicMock(spec=feedparser.FeedParserDict)
            entry.get = MagicMock(side_effect=lambda k, d: d)
            entry.published_parsed = None

            result = extract_entry_fields(entry, "source")

            assert result["title"] == "N/A"
            assert result["url"] == "N/A"
            assert result["summary"] == "N/A"

    # ===== Tests: fetch_feed =====

    class TestFetchFeed:
        """Tests for fetch_feed()."""

        @patch("rss_extract_live.requests.get")
        @patch("rss_extract_live.feedparser.parse")
        def test_successful_feed_fetch(self, mock_parse, mock_get):
            """Should fetch and parse feed successfully."""
            from rss_extract_live import fetch_feed

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = b"<feed></feed>"
            mock_get.return_value = mock_response

            mock_feed = MagicMock()
            mock_feed.entries = [MagicMock()]
            mock_parse.return_value = mock_feed

            result = fetch_feed("https://example.com/feed")

            assert result is not None
            mock_get.assert_called_once()

        @patch("rss_extract_live.requests.get")
        def test_handles_connection_timeout(self, mock_get):
            """Should return None on connection timeout."""
            from rss_extract_live import fetch_feed

            mock_get.side_effect = Exception("Timeout")

            result = fetch_feed("https://example.com/feed")

            assert result is None

        @patch("rss_extract_live.requests.get")
        def test_handles_non_200_status_code(self, mock_get):
            """Should return None for non-200 status codes."""
            from rss_extract_live import fetch_feed

            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response

            result = fetch_feed("https://example.com/feed")

            assert result is None

        @patch("rss_extract_live.requests.get")
        @patch("rss_extract_live.feedparser.parse")
        def test_handles_empty_feed(self, mock_parse, mock_get):
            """Should return None for feed with no entries."""
            from rss_extract_live import fetch_feed

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = b"<feed></feed>"
            mock_get.return_value = mock_response

            mock_feed = MagicMock()
            mock_feed.entries = []
            mock_parse.return_value = mock_feed

            result = fetch_feed("https://example.com/feed")

            assert result is None

    # ===== Tests: get_latest_article_date =====

    class TestGetLatestArticleDate:
        """Tests for get_latest_article_date()."""

        @patch("rss_extract_live.get_connection")
        def test_returns_latest_date_from_rds(self, mock_get_conn):
            """Should return latest published_date from RDS."""
            from rss_extract_live import get_latest_article_date

            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = ("2026-03-26 15:00:00",)
            mock_conn = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_conn.return_value = mock_conn

            result = get_latest_article_date()

            assert result is not None
            assert isinstance(result, pd.Timestamp)

        @patch("rss_extract_live.get_connection")
        def test_returns_none_when_no_articles_in_db(self, mock_get_conn):
            """Should return None when database is empty."""
            from rss_extract_live import get_latest_article_date

            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (None,)
            mock_conn = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_conn.return_value = mock_conn

            result = get_latest_article_date()

            assert result is None

        @patch("rss_extract_live.get_connection")
        def test_handles_db_connection_error(self, mock_get_conn):
            """Should return None on database connection error."""
            from rss_extract_live import get_latest_article_date

            mock_get_conn.side_effect = Exception("Connection failed")

            result = get_latest_article_date()

            assert result is None

        @patch("rss_extract_live.get_connection")
        def test_handles_timezone_aware_timestamps(self, mock_get_conn):
            """Should handle timezone-aware timestamps from RDS."""
            from rss_extract_live import get_latest_article_date

            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = ("2026-03-26 15:00:00+00:00",)
            mock_conn = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_conn.return_value = mock_conn

            result = get_latest_article_date()

            assert result is not None

    # ===== Tests: extract_live =====

    class TestExtractLive:
        """Tests for extract_live()."""

        @patch("rss_extract_live.get_latest_article_date")
        @patch("rss_extract_live.fetch_feed")
        def test_extracts_all_feeds(self, mock_fetch, mock_get_latest):
            """Should extract articles from all feeds."""
            from rss_extract_live import extract_live

            mock_get_latest.return_value = None

            entry = MagicMock()
            entry.title = "News"
            entry.link = "https://example.com"
            entry.get = MagicMock(side_effect=lambda k, d: d)
            entry.published_parsed = (2026, 3, 26, 10, 0, 0, 0, 0, 0)

            mock_feed = MagicMock()
            mock_feed.entries = [entry]
            mock_fetch.return_value = mock_feed

            feeds = {"source1": "url1", "source2": "url2"}
            result = extract_live(feeds)

            assert len(result) == 2
            assert mock_fetch.call_count == 2

        @patch("rss_extract_live.get_latest_article_date")
        @patch("rss_extract_live.fetch_feed")
        def test_filters_articles_by_date(self, mock_fetch, mock_get_latest):
            """Should skip articles older than latest in RDS."""
            from rss_extract_live import extract_live

            latest_date = pd.Timestamp("2026-03-26 12:00:00", tz="UTC")
            mock_get_latest.return_value = latest_date

            entry = MagicMock()
            entry.title = "Old news"
            entry.link = "https://example.com/old"
            entry.get = MagicMock(side_effect=lambda k, d: d)
            entry.published_parsed = (2026, 3, 26, 10, 0, 0, 0, 0, 0)

            mock_feed = MagicMock()
            mock_feed.entries = [entry]
            mock_fetch.return_value = mock_feed

            result = extract_live({"source": "url"})

            assert len(result) == 0

        @patch("rss_extract_live.get_latest_article_date")
        @patch("rss_extract_live.fetch_feed")
        def test_handles_failed_feed_fetch(self, mock_fetch, mock_get_latest):
            """Should skip feeds that fail to fetch."""
            from rss_extract_live import extract_live

            mock_get_latest.return_value = None
            mock_fetch.return_value = None

            result = extract_live({"source": "url"})

            assert len(result) == 0

        @patch("rss_extract_live.get_latest_article_date")
        @patch("rss_extract_live.fetch_feed")
        def test_preserves_article_source(self, mock_fetch, mock_get_latest):
            """Should preserve source name in extracted articles."""
            from rss_extract_live import extract_live

            mock_get_latest.return_value = None

            entry = MagicMock()
            entry.title = "News"
            entry.link = "https://example.com"
            entry.get = MagicMock(side_effect=lambda k, d: d)
            entry.published_parsed = (2026, 3, 26, 10, 0, 0, 0, 0, 0)

            mock_feed = MagicMock()
            mock_feed.entries = [entry]
            mock_fetch.return_value = mock_feed

            result = extract_live({"techcrunch": "url"})

            assert result[0]["source"] == "techcrunch"


# ===== HISTORICAL EXTRACTION TESTS =====

class TestHistoricalExtraction:
    """Tests for historical Hacker News extraction functions."""

    # ===== Tests: get_hn_historical =====

    class TestGetHnHistorical:
        """Tests for get_hn_historical()."""

        @patch("seed_historical.rss_extract_historical.requests.get")
        def test_fetches_hn_stories_successfully(self, mock_get, sample_hn_hit):
            """Should fetch Hacker News stories via Algolia API."""
            from seed_historical.rss_extract_historical import get_hn_historical

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"hits": [sample_hn_hit]}
            mock_get.return_value = mock_response

            result = get_hn_historical("Apple")

            assert len(result) > 0
            assert result[0]["title"] == "Apple releases new AI features"
            assert result[0]["source"] == "hackernews"

        @patch("seed_historical.rss_extract_historical.requests.get")
        def test_limits_results_to_max_results(self, mock_get, sample_hn_hit):
            """Should limit results to HN_MAX_RESULTS."""
            from seed_historical.rss_extract_historical import get_hn_historical, HN_MAX_RESULTS

            hits = [sample_hn_hit] * (HN_MAX_RESULTS + 10)
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"hits": hits}
            mock_get.return_value = mock_response

            result = get_hn_historical("Apple")

            assert len(result) <= HN_MAX_RESULTS

        @patch("seed_historical.rss_extract_historical.requests.get")
        def test_handles_api_failure(self, mock_get):
            """Should return empty list on API error."""
            from seed_historical.rss_extract_historical import get_hn_historical

            mock_response = MagicMock()
            mock_response.status_code = 503
            mock_get.return_value = mock_response

            result = get_hn_historical("Apple")

            assert result == []

        @patch("seed_historical.rss_extract_historical.requests.get")
        def test_handles_network_error(self, mock_get):
            """Should return empty list on network error."""
            from seed_historical.rss_extract_historical import get_hn_historical

            mock_get.side_effect = Exception("Network error")

            result = get_hn_historical("Apple")

            assert result == []

        @patch("seed_historical.rss_extract_historical.requests.get")
        def test_formats_published_date_from_timestamp(self, mock_get):
            """Should convert Unix timestamp to YYYY-MM-DD HH:MM:SS."""
            from seed_historical.rss_extract_historical import get_hn_historical

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "hits": [{
                    "title": "Test",
                    "url": "https://example.com",
                    "points": 100,
                    "num_comments": 10,
                    "created_at_i": 1711497600,
                }]
            }
            mock_get.return_value = mock_response

            result = get_hn_historical("Apple")

            assert len(result[0]["published_date"]) == 19
            assert result[0]["published_date"].count("-") == 2

        @patch("seed_historical.rss_extract_historical.requests.get")
        def test_includes_engagement_metrics_in_summary(self, mock_get):
            """Should include points and comments in summary."""
            from seed_historical.rss_extract_historical import get_hn_historical

            hit = {
                "title": "News story",
                "points": 250,
                "num_comments": 45,
                "created_at_i": 1711497600,
            }
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"hits": [hit]}
            mock_get.return_value = mock_response

            result = get_hn_historical("Apple")

            assert "250" in result[0]["summary"]
            assert "45" in result[0]["summary"]

    # ===== Tests: _extract_for_ticker =====

    class TestExtractForTicker:
        """Tests for _extract_for_ticker()."""

        @patch("seed_historical.rss_extract_historical.get_hn_historical")
        def test_tags_articles_with_ticker(self, mock_get_hn):
            """Should add ticker to each article."""
            from seed_historical.rss_extract_historical import _extract_for_ticker

            mock_article = {
                "title": "News",
                "url": "https://example.com",
                "summary": "Summary",
                "published_date": "2026-03-26 10:00:00",
                "source": "hackernews",
            }
            mock_get_hn.return_value = [mock_article]

            result = _extract_for_ticker(("AAPL", "Apple"))

            assert len(result) == 1
            assert result[0]["ticker"] == "AAPL"

        @patch("seed_historical.rss_extract_historical.get_hn_historical")
        def test_returns_empty_when_no_articles(self, mock_get_hn):
            """Should return empty list when no articles found."""
            from seed_historical.rss_extract_historical import _extract_for_ticker

            mock_get_hn.return_value = []

            result = _extract_for_ticker(("AAPL", "Apple"))

            assert result == []

        @patch("seed_historical.rss_extract_historical.get_hn_historical")
        def test_preserves_article_fields(self, mock_get_hn):
            """Should preserve all original article fields."""
            from seed_historical.rss_extract_historical import _extract_for_ticker

            mock_article = {
                "title": "News",
                "url": "https://example.com",
                "summary": "Summary",
                "published_date": "2026-03-26 10:00:00",
                "source": "hackernews",
            }
            mock_get_hn.return_value = [mock_article]

            result = _extract_for_ticker(("AAPL", "Apple"))

            assert result[0]["title"] == "News"
            assert result[0]["url"] == "https://example.com"
            assert result[0]["summary"] == "Summary"

    # ===== Tests: extract_historical =====

    class TestExtractHistorical:
        """Tests for extract_historical()."""

        @patch("seed_historical.rss_extract_historical._extract_for_ticker")
        def test_extracts_from_all_tickers(self, mock_extract_ticker, sample_ticker_map):
            """Should extract from all tickers in map."""
            from seed_historical.rss_extract_historical import extract_historical

            mock_extract_ticker.return_value = [
                {"ticker": "AAPL", "title": "News"}
            ]

            result = extract_historical(sample_ticker_map)

            assert mock_extract_ticker.call_count == len(sample_ticker_map)

        @patch("seed_historical.rss_extract_historical._extract_for_ticker")
        def test_flattens_results(self, mock_extract_ticker, sample_ticker_map):
            """Should flatten list of lists into single list."""
            from seed_historical.rss_extract_historical import extract_historical

            mock_extract_ticker.return_value = [
                {"ticker": "AAPL", "title": "News"}]

            result = extract_historical(sample_ticker_map)

            assert isinstance(result, list)
            assert all(isinstance(item, dict) for item in result)

        @patch("seed_historical.rss_extract_historical._extract_for_ticker")
        def test_aggregates_all_articles(self, mock_extract_ticker, sample_ticker_map):
            """Should aggregate articles from all tickers."""
            from seed_historical.rss_extract_historical import extract_historical

            mock_extract_ticker.side_effect = [
                [{"ticker": "AAPL", "title": "News1"}],
                [{"ticker": "MSFT", "title": "News2"}],
                [{"ticker": "GOOGL", "title": "News3"}],
            ]

            result = extract_historical(sample_ticker_map)

            assert len(result) == 3
            tickers = [item["ticker"] for item in result]
            assert "AAPL" in tickers
            assert "MSFT" in tickers
            assert "GOOGL" in tickers

        @patch("seed_historical.rss_extract_historical._extract_for_ticker")
        def test_handles_empty_ticker_map(self, mock_extract_ticker):
            """Should handle empty ticker map gracefully."""
            from seed_historical.rss_extract_historical import extract_historical

            result = extract_historical({})

            assert result == []
            mock_extract_ticker.assert_not_called()

        @patch("seed_historical.rss_extract_historical._extract_for_ticker")
        def test_handles_failed_extraction_for_ticker(self, mock_extract_ticker, sample_ticker_map):
            """Should skip tickers with failed extraction."""
            from seed_historical.rss_extract_historical import extract_historical

            mock_extract_ticker.side_effect = [[], [], []]

            result = extract_historical(sample_ticker_map)

            assert result == []
