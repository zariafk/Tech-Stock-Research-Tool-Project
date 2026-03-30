"""Unit tests for the transform script."""

import pandas as pd
import pytest

from transform import (
    flatten_post_data,
    drop_missing_required,
    validate_numeric_ranges,
    convert_timestamps,
    build_fact_posts,
    build_dim_subreddits,
    transform_main,
)


class TestFlattenPostData:
    """Tests for flatten_post_data."""

    def test_extracts_data_dicts(self, raw_posts):
        df = flatten_post_data(raw_posts)
        assert len(df) == 3
        assert "id" in df.columns

    def test_skips_posts_without_data_key(self):
        posts = [{"kind": "t3"}, {"data": {"id": "abc"}}]
        df = flatten_post_data(posts)
        assert len(df) == 1

    def test_empty_input(self):
        df = flatten_post_data([])
        assert len(df) == 0


class TestDropMissingRequired:
    """Tests for drop_missing_required."""

    def test_drops_null_required_fields(self, required_columns):
        df = pd.DataFrame([
            {"id": "a", "title": "Post", "subreddit_id": "t5_1", "author": "user1"},
            {"id": "b", "title": None, "subreddit_id": "t5_1", "author": "user2"},
        ])
        result = drop_missing_required(df, required_columns)
        assert len(result) == 1
        assert result.iloc[0]["id"] == "a"

    def test_drops_deleted_authors(self, required_columns):
        df = pd.DataFrame([
            {"id": "a", "title": "Post",
                "subreddit_id": "t5_1", "author": "[deleted]"},
            {"id": "b", "title": "Post", "subreddit_id": "t5_1", "author": "real_user"},
        ])
        result = drop_missing_required(df, required_columns)
        assert len(result) == 1
        assert result.iloc[0]["author"] == "real_user"

    def test_drops_removed_titles(self, required_columns):
        df = pd.DataFrame([
            {"id": "a", "title": "[removed]",
                "subreddit_id": "t5_1", "author": "user1"},
        ])
        result = drop_missing_required(df, required_columns)
        assert len(result) == 0

    def test_keeps_all_valid_rows(self, required_columns):
        df = pd.DataFrame([
            {"id": "a", "title": "Post", "subreddit_id": "t5_1", "author": "user1"},
            {"id": "b", "title": "Post2", "subreddit_id": "t5_2", "author": "user2"},
        ])
        result = drop_missing_required(df, required_columns)
        assert len(result) == 2


class TestValidateNumericRanges:
    """Tests for validate_numeric_ranges."""

    def test_clips_negative_scores(self):
        df = pd.DataFrame([{
            "score": -10, "ups": -5, "num_comments": -1,
            "upvote_ratio": -0.5, "subreddit_subscribers": -100,
        }])
        result = validate_numeric_ranges(df)
        assert result.iloc[0]["score"] == 0
        assert result.iloc[0]["ups"] == 0
        assert result.iloc[0]["num_comments"] == 0
        assert result.iloc[0]["upvote_ratio"] == 0.0
        assert result.iloc[0]["subreddit_subscribers"] == 0

    def test_clips_upvote_ratio_above_one(self):
        df = pd.DataFrame([{
            "score": 10, "ups": 10, "num_comments": 5,
            "upvote_ratio": 1.5, "subreddit_subscribers": 1000,
        }])
        result = validate_numeric_ranges(df)
        assert result.iloc[0]["upvote_ratio"] == 1.0

    def test_leaves_valid_values_unchanged(self):
        df = pd.DataFrame([{
            "score": 50, "ups": 45, "num_comments": 10,
            "upvote_ratio": 0.9, "subreddit_subscribers": 5000,
        }])
        result = validate_numeric_ranges(df)
        assert result.iloc[0]["score"] == 50
        assert result.iloc[0]["upvote_ratio"] == 0.9


class TestConvertTimestamps:
    """Tests for convert_timestamps."""

    def test_converts_unix_to_iso(self):
        df = pd.DataFrame([{"created_utc": 1710000000}])
        result = convert_timestamps(df)
        assert result.iloc[0]["created_utc"] == "2024-03-09T16:00:00Z"

    def test_handles_multiple_rows(self):
        df = pd.DataFrame([
            {"created_utc": 1710000000},
            {"created_utc": 1710086400},
        ])
        result = convert_timestamps(df)
        assert len(result) == 2
        assert result.iloc[0]["created_utc"].endswith("Z")
        assert result.iloc[1]["created_utc"].endswith("Z")


class TestBuildFactPosts:
    """Tests for build_fact_posts."""

    def test_selects_correct_columns(self, raw_posts, fact_columns):
        df = flatten_post_data(raw_posts)
        fact = build_fact_posts(df, fact_columns)
        assert list(fact.columns) == fact_columns

    def test_deduplicates_by_id(self, fact_columns):
        df = pd.DataFrame([
            {"id": "a", "title": "Post 1"},
            {"id": "a", "title": "Post 1 duplicate"},
        ])
        # Fill remaining columns so reindex doesn't break
        for col in fact_columns:
            if col not in df.columns:
                df[col] = "test"
        fact = build_fact_posts(df, fact_columns)
        assert len(fact) == 1


class TestBuildDimSubreddits:
    """Tests for build_dim_subreddits."""

    def test_deduplicates_by_subreddit_id(self, raw_posts, dim_columns):
        # raw_posts has two posts from "stocks" with the same subreddit_id
        df = flatten_post_data(raw_posts)
        dim = build_dim_subreddits(df, dim_columns)
        assert len(dim) == 2  # stocks and options

    def test_keeps_highest_subscriber_count(self, raw_posts, dim_columns):
        df = flatten_post_data(raw_posts)
        dim = build_dim_subreddits(df, dim_columns)
        stocks_row = dim[dim["subreddit_id"] == "t5_2qjfk"]
        # The third post has 5000100 subscribers, higher than 5000000
        assert stocks_row.iloc[0]["subreddit_subscribers"] == 5000100

    def test_renames_subreddit_to_subreddit_name(self, raw_posts, dim_columns):
        df = flatten_post_data(raw_posts)
        dim = build_dim_subreddits(df, dim_columns)
        assert "subreddit_name" in dim.columns
        assert "subreddit" not in dim.columns


class TestTransformMain:
    """Tests for the full transform pipeline."""

    def test_returns_two_dataframes(
        self, raw_posts, fact_columns, dim_columns, required_columns,
    ):
        fact, dim = transform_main(
            raw_posts,
            fact_columns=fact_columns,
            dim_columns=dim_columns,
            required_columns=required_columns,
        )
        assert isinstance(fact, pd.DataFrame)
        assert isinstance(dim, pd.DataFrame)

    def test_empty_input_returns_empty_frames(
        self, fact_columns, dim_columns, required_columns,
    ):
        fact, dim = transform_main(
            [],
            fact_columns=fact_columns,
            dim_columns=dim_columns,
            required_columns=required_columns,
        )
        assert len(fact) == 0
        assert len(dim) == 0

    def test_timestamps_are_converted(
        self, raw_posts, fact_columns, dim_columns, required_columns,
    ):
        fact, _ = transform_main(
            raw_posts,
            fact_columns=fact_columns,
            dim_columns=dim_columns,
            required_columns=required_columns,
        )
        assert fact.iloc[0]["created_utc"].endswith("Z")
