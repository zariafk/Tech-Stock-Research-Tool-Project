"""Historical Reddit data extract script using Arctic Shift API."""

import time
import logging
from datetime import datetime, timezone, timedelta

import requests


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://arctic-shift.photon-reddit.com/api/posts/search"
REQUEST_TIMEOUT = 30


def fetch_day(
    subreddit: str,
    date: datetime,
    *,
    limit: int = 100,
    retries: int = 3,
) -> list[dict]:
    """Fetches all posts for a single subreddit on a single day."""
    start = date.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    params = {
        "subreddit": subreddit,
        "after": int(start.timestamp()),
        "before": int(end.timestamp()),
        "limit": limit,
    }

    all_posts = []

    for attempt in range(retries):
        response = requests.get(
            BASE_URL, params=params, timeout=REQUEST_TIMEOUT
        )

        if response.ok:
            data = response.json().get("data", [])
            all_posts.extend(data)

            # Arctic Shift paginates; stop when fewer results than limit
            if len(data) < limit:
                break

            # Use the last post's created_utc to paginate forward
            params["after"] = data[-1]["created_utc"]
            continue

        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 5 * (attempt + 1)))
            logger.warning("Rate limited, waiting %s seconds", wait)
            time.sleep(wait)
        else:
            logger.error("Request failed: %s", response.status_code)
            break

    return all_posts


def generate_date_range(
    start_date: datetime,
    end_date: datetime,
) -> list[datetime]:
    """Generates a list of dates from start to end inclusive."""
    days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=i) for i in range(days)]


def extract_historical(
    subreddits: list[str],
    *,
    start_date: datetime,
    end_date: datetime,
    delay: float = 1.0,
) -> list[dict]:
    """Extracts historical posts for all subreddits across a date range."""
    dates = generate_date_range(start_date, end_date)
    all_posts = []

    for subreddit in subreddits:
        for date in dates:
            logger.info(
                "Fetching r/%s for %s",
                subreddit, date.strftime("%Y-%m-%d"),
            )

            posts = fetch_day(subreddit, date)

            # Wrap in the same {"data": ...} structure as the live extractor
            # so transform_main can process both formats identically
            all_posts.extend({"data": post} for post in posts)

            time.sleep(delay)

        logger.info(
            "Completed r/%s — %d posts so far", subreddit, len(all_posts)
        )

    logger.info("Historical extract complete — %d total posts", len(all_posts))
    return all_posts
