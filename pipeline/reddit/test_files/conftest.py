"""Shared fixtures for the Reddit pipeline test suite."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture()
def raw_posts():
    """Mimics the structure returned by the Reddit JSON API / extract script."""
    return [
        {
            "kind": "t3",
            "data": {
                "id": "abc123",
                "title": "Stock picks for today",
                "selftext": "Here are my picks...",
                "link_flair_text": "Discussion",
                "score": 150,
                "ups": 140,
                "upvote_ratio": 0.92,
                "num_comments": 45,
                "author": "trader_joe",
                "created_utc": 1710000000,
                "permalink": "/r/stocks/comments/abc123/stock_picks/",
                "url": "https://reddit.com/r/stocks/comments/abc123/",
                "subreddit_id": "t5_2qjfk",
                "subreddit": "stocks",
                "subreddit_subscribers": 5000000,
            },
        },
        {
            "kind": "t3",
            "data": {
                "id": "def456",
                "title": "Options strategy thread",
                "selftext": "Let's discuss spreads...",
                "link_flair_text": "Strategy",
                "score": 85,
                "ups": 80,
                "upvote_ratio": 0.88,
                "num_comments": 22,
                "author": "options_guru",
                "created_utc": 1710086400,
                "permalink": "/r/options/comments/def456/options_strategy/",
                "url": "https://reddit.com/r/options/comments/def456/",
                "subreddit_id": "t5_2qh8m",
                "subreddit": "options",
                "subreddit_subscribers": 1200000,
            },
        },
        {
            "kind": "t3",
            "data": {
                "id": "ghi789",
                "title": "Another stocks post",
                "selftext": "More analysis...",
                "link_flair_text": None,
                "score": 200,
                "ups": 190,
                "upvote_ratio": 0.95,
                "num_comments": 60,
                "author": "bull_market",
                "created_utc": 1710172800,
                "permalink": "/r/stocks/comments/ghi789/another_post/",
                "url": "https://reddit.com/r/stocks/comments/ghi789/",
                "subreddit_id": "t5_2qjfk",
                "subreddit": "stocks",
                "subreddit_subscribers": 5000100,
            },
        },
    ]


@pytest.fixture()
def fact_columns():
    """Column list for the FACT_posts table."""
    return [
        "id", "title", "selftext", "link_flair_text", "score",
        "ups", "upvote_ratio", "num_comments", "author",
        "created_utc", "permalink", "url", "subreddit_id",
    ]


@pytest.fixture()
def dim_columns():
    """Column list for the DIM_subreddits table."""
    return ["subreddit_id", "subreddit", "subreddit_subscribers"]


@pytest.fixture()
def required_columns():
    """Columns that must not be null."""
    return ["id", "title", "subreddit_id", "author"]
