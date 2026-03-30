"""Unit tests for the Reddit extract module."""

import pytest
from unittest.mock import patch, MagicMock
from extract import RedditExtractor, extract_main


# === Fixtures ===

@pytest.fixture
def extractor():
    """Default RedditExtractor for r/stocks."""
    return RedditExtractor("stocks")


def make_response(status: int, json_data: dict = None, headers: dict = None) -> MagicMock:
    """Build a mock requests.Response."""
    response = MagicMock()
    response.status_code = status
    response.ok = status == 200
    response.json.return_value = json_data or {}
    response.headers = headers or {}
    return response


def make_post_response(num_posts: int = 2) -> dict:
    """Build a minimal Reddit listing JSON with num_posts children."""
    children = [
        {"kind": "t3", "data": {"id": f"post{i}",
                                "title": f"Title {i}", "ups": 100 * i}}
        for i in range(num_posts)
    ]
    return {"data": {"children": children}}


# === base_url ===

class TestBaseUrl:
    """Tests for the base_url property."""

    def test_base_url_format(self, extractor):
        assert extractor.base_url == "https://www.reddit.com/r/stocks/hot.json"

    def test_base_url_uses_sort_type(self):
        extractor = RedditExtractor("stocks", sort_type="new")
        assert "/new.json" in extractor.base_url


# === build_params ===

class TestBuildParams:
    """Tests for build_params()."""

    def test_hot_sort_has_no_time_filter(self, extractor):
        params = extractor.build_params()
        assert "t" not in params

    def test_top_sort_includes_time_filter(self):
        extractor = RedditExtractor(
            "stocks", sort_type="top", time_filter="week")
        params = extractor.build_params()
        assert params["t"] == "week"

    def test_limit_in_params(self, extractor):
        assert extractor.build_params()["limit"] == 25


# === get_post_data ===

class TestGetPostData:
    """Tests for get_post_data()."""

    def test_successful_response_returns_json(self, extractor):
        expected = make_post_response()
        with patch("extract.requests.get", return_value=make_response(200, expected)):
            result = extractor.get_post_data()
        assert result == expected

    def test_all_failures_returns_empty_dict(self, extractor):
        with patch("extract.requests.get", return_value=make_response(500)):
            result = extractor.get_post_data(retries=2)
        assert result == {}

    def test_rate_limit_retries(self, extractor):
        rate_limited = make_response(429, headers={"Retry-After": "0"})
        success = make_response(200, make_post_response())
        with patch("extract.requests.get", side_effect=[rate_limited, success]), \
                patch("extract.time.sleep"):
            result = extractor.get_post_data()
        assert "data" in result


# === get_comment_data ===

class TestGetCommentData:
    """Tests for get_comment_data()."""

    def make_comment_response(self, ups: int = 10) -> dict:
        comment = {"kind": "t1", "data": {"body": "Great post", "ups": ups}}
        return [{}, {"data": {"children": [comment]}}]

    def test_returns_comments_above_min_upvotes(self, extractor):
        json_data = self.make_comment_response(ups=10)
        with patch("extract.requests.get", return_value=make_response(200, json_data)):
            result = extractor.get_comment_data("abc123", min_upvotes=5)
        assert len(result) == 1

    def test_filters_comments_below_min_upvotes(self, extractor):
        json_data = self.make_comment_response(ups=1)
        with patch("extract.requests.get", return_value=make_response(200, json_data)):
            result = extractor.get_comment_data("abc123", min_upvotes=5)
        assert result == []

    def test_all_failures_returns_empty_list(self, extractor):
        with patch("extract.requests.get", return_value=make_response(500)):
            result = extractor.get_comment_data("abc123")
        assert result == []
