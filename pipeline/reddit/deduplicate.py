"""Deduplication script — removes already-seen posts from raw extract data."""

import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def deduplicate_raw_posts(
    raw_posts: list[dict],
    existing_ids: set[str],
) -> list[dict]:
    """Filters out posts whose ID already exists in the database."""
    before = len(raw_posts)

    new_posts = [
        post for post in raw_posts
        if post.get("data", {}).get("id") not in existing_ids
    ]

    removed = before - len(new_posts)
    logger.info(
        "Deduplication: %d total, %d duplicates removed, %d new",
        before, removed, len(new_posts),
    )

    return new_posts
