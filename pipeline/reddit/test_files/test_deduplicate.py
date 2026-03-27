"""Unit tests for the deduplication script."""

from deduplicate import deduplicate_raw_posts


class TestDeduplicateRawPosts:
    """Tests for deduplicate_raw_posts."""

    def test_removes_known_ids(self, raw_posts):
        existing = {"abc123"}
        result = deduplicate_raw_posts(raw_posts, existing)
        assert len(result) == 2
        result_ids = {p["data"]["id"] for p in result}
        assert "abc123" not in result_ids

    def test_removes_multiple_known_ids(self, raw_posts):
        existing = {"abc123", "def456"}
        result = deduplicate_raw_posts(raw_posts, existing)
        assert len(result) == 1
        assert result[0]["data"]["id"] == "ghi789"

    def test_keeps_all_when_no_matches(self, raw_posts):
        existing = {"zzz999"}
        result = deduplicate_raw_posts(raw_posts, existing)
        assert len(result) == 3

    def test_removes_all_when_all_match(self, raw_posts):
        existing = {"abc123", "def456", "ghi789"}
        result = deduplicate_raw_posts(raw_posts, existing)
        assert len(result) == 0

    def test_empty_existing_ids(self, raw_posts):
        result = deduplicate_raw_posts(raw_posts, set())
        assert len(result) == 3

    def test_empty_raw_posts(self):
        result = deduplicate_raw_posts([], {"abc123"})
        assert len(result) == 0

    def test_handles_missing_data_key(self):
        posts = [{"kind": "t3"}, {"data": {"id": "abc"}}]
        existing = {"abc"}
        result = deduplicate_raw_posts(posts, existing)
        # The post without "data" has no id, so it doesn't match and stays
        assert len(result) == 1
