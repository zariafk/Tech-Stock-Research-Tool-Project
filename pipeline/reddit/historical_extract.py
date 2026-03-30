"""Historical Reddit data extract script using Arctic Shift API."""

import time
import logging
from datetime import datetime, timezone, timedelta

import requests


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://arctic-shift.photon-reddit.com/api/posts/search"
REQUEST_TIMEOUT = 30
DEFAULT_MAX_POSTS = 10


def fetch_day(
    subreddit: str,
    date: datetime,
    *,
    page_size: int = 100,
    max_posts: int = DEFAULT_MAX_POSTS,
    retries: int = 3,
) -> list[dict]:
    """Fetches posts for a single subreddit on a single day.

    Retrieves all posts for the day, ranks them by engagement
    (score + num_comments), and returns the top `max_posts`.
    This ensures we keep the most relevant posts rather than
    just the first ones chronologically.
    """
    start = date.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    params = {
        "subreddit": subreddit,
        "after": int(start.timestamp()),
        "before": int(end.timestamp()),
        "limit": page_size,
    }

    all_posts = []

    while True:
        success = False

        for attempt in range(retries):
            response = requests.get(
                BASE_URL, params=params, timeout=REQUEST_TIMEOUT
            )

            if response.ok:
                data = response.json().get("data", [])
                all_posts.extend(data)
                success = True
                break

            if response.status_code == 429:
                wait = int(
                    response.headers.get("Retry-After", 5 * (attempt + 1))
                )
                logger.warning("Rate limited, waiting %s seconds", wait)
                time.sleep(wait)
            else:
                logger.error("Request failed: %s", response.status_code)
                break

        if not success or len(data) < page_size:
            break

        # Paginate forward using the last post's timestamp
        params["after"] = data[-1]["created_utc"]

    # Rank by engagement and keep the top max_posts
    if len(all_posts) > max_posts:
        all_posts.sort(
            key=lambda p: p.get("score", 0) + p.get("num_comments", 0),
            reverse=True,
        )
        logger.info(
            "r/%s %s: trimmed %d posts to top %d by engagement",
            subreddit, date.strftime("%Y-%m-%d"),
            len(all_posts), max_posts,
        )
        all_posts = all_posts[:max_posts]

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
    max_posts: int = DEFAULT_MAX_POSTS,
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

            posts = fetch_day(subreddit, date, max_posts=max_posts)

            all_posts.extend({"data": post} for post in posts)

            time.sleep(delay)

        logger.info(
            "Completed r/%s — %d posts so far", subreddit, len(all_posts)
        )

    logger.info("Historical extract complete — %d total posts", len(all_posts))
    return all_posts
