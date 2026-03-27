"""Unit tests for RSS extraction orchestrator."""

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import json


# ===== Fixtures =====

@pytest.fixture
def mock_live_articles():
    """Mock live RSS extraction."""
    return [
        {
            "title": "Apple announces new chip",
            "link": "https://techcrunch.com/apple-chip",
            "summary": "Apple unveiled M4 processor today.",
            "published_date": "2026-03-26 10:00:00",
            "source": "techcrunch",
        }
    ]


@pytest.fixture
def mock_historical_articles():
    """Mock historical HN extraction."""
    return [
        {
            "title": "Microsoft pushes AI limits",
            "link": "https://news.ycombinator.com/microsoft-ai",
            "summary": "Points: 500",
            "published_date": "2025-06-15 14:30:00",
            "source": "algolia_hn",
        }
    ]


@pytest.fixture
def sample_article():
    """Single article for testing."""
    return {
        "title": "Apple launches new M4 chip",
        "link": "https://example.com/apple-m4",
        "summary": "AAPL announced the M4 processor with 10 cores.",
        "published_date": "2026-03-26 10:00:00",
        "source": "techcrunch",
    }


# ===== Tests: format_ticker_prompt =====

class TestFormatTickerPrompt:
    """Tests for format_ticker_prompt()."""

    def test_prompt_includes_tickers(self, sample_article):
        """Prompt should list all tickers in the universe."""
        from pipeline.rss.rss_analysis import format_ticker_prompt

        prompt = format_ticker_prompt(sample_article, ["AAPL", "MSFT"])
        assert "AAPL" in prompt
        assert "MSFT" in prompt

    def test_prompt_includes_article_content(self, sample_article):
        """Prompt should include title and summary."""
        from pipeline.rss.rss_analysis import format_ticker_prompt

        prompt = format_ticker_prompt(sample_article, ["AAPL"])
        assert sample_article["title"] in prompt
        assert sample_article["summary"] in prompt

    def test_prompt_specifies_json_output(self, sample_article):
        """Prompt should request JSON format."""
        from pipeline.rss.rss_analysis import format_ticker_prompt

        prompt = format_ticker_prompt(sample_article, ["AAPL"])
        assert "JSON" in prompt
        assert "TICKER" in prompt or '"t"' in prompt


# ===== Tests: extract_keywords =====

class TestExtractKeywords:
    """Tests for extract_keywords()."""

    def test_matches_ticker_symbol(self, sample_article):
        """Should match ticker symbols in title/summary."""
        from pipeline.rss.rss_analysis import extract_keywords

        matches = extract_keywords(sample_article, ["AAPL", "MSFT"])
        assert "AAPL" in matches

    def test_matches_company_name(self):
        """Should match company names in title/summary."""
        from pipeline.rss.rss_analysis import extract_keywords

        entry = {
            "title": "Apple and Microsoft partner",
            "summary": "Apple Inc announced partnership with Microsoft Corp."
        }
        # Assuming AAPL -> Apple in tech_universe
        matches = extract_keywords(entry, ["AAPL", "MSFT"])
        assert len(matches) > 0

    def test_case_insensitive_matching(self):
        """Should handle case-insensitive matching."""
        from pipeline.rss.rss_analysis import extract_keywords

        entry = {
            "title": "APPLE stock rises",
            "summary": "Apple Inc stock up today"
        }
        matches = extract_keywords(entry, ["AAPL"])
        assert "AAPL" in matches

    def test_returns_empty_if_no_matches(self):
        """Should return empty list if no tickers mentioned."""
        from pipeline.rss.rss_analysis import extract_keywords

        entry = {
            "title": "Weather forecast for London",
            "summary": "Rain expected tomorrow"
        }
        matches = extract_keywords(entry, ["AAPL", "MSFT"])
        assert matches == []


# ===== Tests: parse_relevance_data =====

class TestParseRelevanceData:
    """Tests for parse_relevance_data()."""

    def test_parses_valid_json_response(self):
        """Should parse valid JSON with all fields."""
        from pipeline.rss.rss_analysis import parse_relevance_data

        response = '[{"t": "AAPL", "r": 9, "s": 0.85, "why": "Product launch"}]'
        results = parse_relevance_data(response)

        assert len(results) == 1
        assert results[0]["ticker"] == "AAPL"
        assert results[0]["score"] == 9
        assert results[0]["sentiment"] == 0.85
        assert "Product" in results[0]["analysis"]

    def test_filters_scores_below_7(self):
        """Should only return items with score >= 7."""
        from pipeline.rss.rss_analysis import parse_relevance_data

        response = '[{"t": "AAPL", "r": 9, "s": 0.8, "why": "Strong"}, {"t": "MSFT", "r": 5, "s": -0.2, "why": "Weak"}]'
        results = parse_relevance_data(response)

        assert len(results) == 1
        assert results[0]["ticker"] == "AAPL"

    def test_handles_malformed_json(self):
        """Should return empty list on JSON parse error."""
        from pipeline.rss.rss_analysis import parse_relevance_data

        response = "This is not valid JSON{}"
        results = parse_relevance_data(response)

        assert results == []

    def test_handles_dict_response(self):
        """Should handle single dict response (not list)."""
        from pipeline.rss.rss_analysis import parse_relevance_data

        response = '{"t": "AAPL", "r": 8, "s": 0.7, "why": "News item"}'
        results = parse_relevance_data(response)

        assert len(results) == 1
        assert results[0]["ticker"] == "AAPL"

    def test_strips_markdown_code_blocks(self):
        """Should strip markdown ```json ``` markers."""
        from pipeline.rss.rss_analysis import parse_relevance_data

        response = '```json\n[{"t": "AAPL", "r": 8, "s": 0.7, "why": "News"}]\n```'
        results = parse_relevance_data(response)

        assert len(results) == 1


# ===== Tests: get_ticker_analysis =====

class TestGetTickerAnalysis:
    """Tests for get_ticker_analysis() with retry logic."""

    @patch("rss_extract.CLIENT")
    def test_successful_analysis_on_first_try(self, mock_client, sample_article):
        """Should return analysis on successful OpenAI call."""
        from pipeline.rss.rss_analysis import get_ticker_analysis

        mock_response = MagicMock()
        mock_response.choices[0].message.content = '[{"t": "AAPL", "r": 9, "s": 0.85, "why": "Launch"}]'
        mock_client.chat.completions.create.return_value = mock_response

        results = get_ticker_analysis(sample_article, ["AAPL"])

        assert len(results) == 1
        assert results[0]["ticker"] == "AAPL"
        assert mock_client.chat.completions.create.call_count == 1

    @patch("rss_extract.time.sleep")
    @patch("rss_extract.CLIENT")
    def test_retries_on_rate_limit(self, mock_client, mock_sleep, sample_article):
        """Should retry with backoff on rate limit error."""
        from pipeline.rss.rss_analysis import get_ticker_analysis

        # First attempt: rate limit, Second attempt: success
        rate_limit_error = Exception("rate_limit_exceeded")
        success_response = MagicMock()
        success_response.choices[0].message.content = '[{"t": "AAPL", "r": 8, "s": 0.7, "why": "Hit"}]'

        mock_client.chat.completions.create.side_effect = [
            rate_limit_error, success_response]

        results = get_ticker_analysis(sample_article, ["AAPL"], max_retries=3)

        assert len(results) == 1
        assert mock_client.chat.completions.create.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2^0 = 1 second backoff

    @patch("rss_extract.CLIENT")
    def test_returns_empty_after_max_retries(self, mock_client, sample_article):
        """Should return empty list after exhausting retries."""
        from pipeline.rss.rss_analysis import get_ticker_analysis

        mock_client.chat.completions.create.side_effect = Exception(
            "rate_limit_exceeded")

        results = get_ticker_analysis(sample_article, ["AAPL"], max_retries=2)

        assert results == []
        assert mock_client.chat.completions.create.call_count == 2


# ===== Tests: filter_by_ticker =====

class TestFilterByTicker:
    """Tests for filter_by_ticker()."""

    @patch("rss_extract.CLIENT")
    def test_skips_articles_with_no_keyword_matches(self, mock_client):
        """Should skip articles that don't mention any tickers."""
        from pipeline.rss.rss_analysis import filter_by_ticker

        article = {
            "title": "Weather in London",
            "summary": "Rain expected tomorrow.",
            "link": "https://example.com"
        }

        results = filter_by_ticker([article], ["AAPL", "MSFT"])

        assert len(results) == 0
        mock_client.chat.completions.create.assert_not_called()

    @patch("rss_extract.CLIENT")
    def test_calls_openai_for_matching_articles(self, mock_client, sample_article):
        """Should call OpenAI for articles with keyword matches."""
        from pipeline.rss.rss_analysis import filter_by_ticker

        mock_response = MagicMock()
        mock_response.choices[0].message.content = '[{"t": "AAPL", "r": 9, "s": 0.85, "why": "News"}]'
        mock_client.chat.completions.create.return_value = mock_response

        results = filter_by_ticker([sample_article], ["AAPL", "MSFT"])

        assert len(results) == 1
        assert results[0]["ticker"] == "AAPL"
        mock_client.chat.completions.create.assert_called_once()


# ===== Tests: create_dataframe =====

class TestCreateDataframe:
    """Tests for create_dataframe()."""

    def test_empty_list_returns_empty_dataframe(self):
        """Should return empty DataFrame for empty article list."""
        from pipeline.rss.rss_analysis import create_dataframe

        df = create_dataframe([])
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_creates_article_id_hash(self, sample_article):
        """Should generate MD5 article_id from link."""
        from pipeline.rss.rss_analysis import create_dataframe

        article_with_result = sample_article.copy()
        article_with_result.update(
            {"score": 9, "sentiment": 0.85, "analysis": "News"})

        df = create_dataframe([article_with_result])

        assert "article_id" in df.columns
        assert len(df["article_id"].iloc[0]) == 32  # MD5 hash length

    def test_includes_all_required_columns(self, sample_article):
        """Should include all required columns."""
        from pipeline.rss.rss_analysis import create_dataframe

        article_with_result = sample_article.copy()
        article_with_result.update(
            {"score": 9, "sentiment": 0.85, "analysis": "News"})

        df = create_dataframe([article_with_result])

        required = ["ticker", "article_id", "title", "link", "summary",
                    "published_date", "source", "score", "sentiment", "analysis"]
        for col in required:
            assert col in df.columns

    def test_converts_published_date_to_datetime(self, sample_article):
        """Should convert published_date to datetime."""
        from pipeline.rss.rss_analysis import create_dataframe

        article_with_result = sample_article.copy()
        article_with_result.update(
            {"score": 9, "sentiment": 0.85, "analysis": "News"})

        df = create_dataframe([article_with_result])

        assert pd.api.types.is_datetime64_any_dtype(df["published_date"])

    def test_sorts_by_ticker_and_date(self, sample_article):
        """Should sort by ticker, then by published_date descending."""
        from pipeline.rss.rss_analysis import create_dataframe

        article1 = sample_article.copy()
        article1.update({"ticker": "AAPL", "score": 9, "sentiment": 0.85,
                        "analysis": "Old", "published_date": "2026-01-01 10:00:00"})

        article2 = sample_article.copy()
        article2["link"] = "https://example.com/new"
        article2.update({"ticker": "AAPL", "score": 8, "sentiment": 0.7,
                        "analysis": "New", "published_date": "2026-03-26 10:00:00"})

        df = create_dataframe([article1, article2])

        # Newer article should come first
        assert df.iloc[0]["published_date"] > df.iloc[1]["published_date"]


# ===== Tests: deduplicate_raw =====

class TestDeduplicateRaw:
    """Tests for deduplicate_raw()."""

    def test_removes_duplicate_links(self):
        """Should remove articles with duplicate links."""
        from pipeline.rss.rss_analysis import deduplicate_raw

        article = {
            "title": "News",
            "link": "https://example.com/news",
            "summary": "Summary",
            "published_date": "2026-03-26 10:00:00",
            "source": "tech"
        }

        results = deduplicate_raw([article, article, article])

        assert len(results) == 1

    def test_preserves_unique_articles(self):
        """Should preserve articles with unique links."""
        from pipeline.rss.rss_analysis import deduplicate_raw

        articles = [
            {"title": "A", "link": "https://a.com", "summary": "1",
                "published_date": "2026-03-26 10:00:00", "source": "tech"},
            {"title": "B", "link": "https://b.com", "summary": "2",
                "published_date": "2026-03-26 10:00:00", "source": "tech"},
            {"title": "C", "link": "https://c.com", "summary": "3",
                "published_date": "2026-03-26 10:00:00", "source": "tech"},
        ]

        results = deduplicate_raw(articles)

        assert len(results) == 3

    def test_preserves_first_occurrence(self):
        """Should keep the first occurrence of duplicate."""
        from pipeline.rss.rss_analysis import deduplicate_raw

        article1 = {"title": "First", "link": "https://example.com",
                    "summary": "1", "published_date": "2026-03-26 10:00:00", "source": "tech"}
        article2 = {"title": "Second", "link": "https://example.com",
                    "summary": "2", "published_date": "2026-03-26 10:00:00", "source": "tech"}

        results = deduplicate_raw([article1, article2])

        assert results[0]["title"] == "First"
