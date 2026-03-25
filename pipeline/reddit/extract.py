"""Reddit data extract script."""

import time
import logging

import requests
import pandas as pd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_mappings(file_path: str = "../company_mapping.csv") -> pd.DataFrame:
    """Loads in the tickers for the stocks of interest."""
    return pd.read_csv(file_path)


class RedditExtractor:
    """
    Each object of this class represents a unique subreddit,
    which can be queried for data containing recent posts.
    """

    VALID_SORTS = {"hot", "new", "top", "rising", "controversial"}
    TIME_FILTERED_SORTS = {"top", "controversial"}

    def __init__(
        self,
        subreddit: str,
        sort_type: str = "hot",
        page_limit: int = 25,
        time_filter: str = "day",
        header: dict | None = None,
    ) -> None:
        if sort_type not in self.VALID_SORTS:
            raise ValueError(f"sort_type must be one of {self.VALID_SORTS}")

        self.subreddit = subreddit
        self.sort_type = sort_type
        self.page_limit = min(page_limit, 100)
        self.time_filter = time_filter
        self.header = header or {"User-Agent": "script:v1.0 (by /u/sigmabot)"}

    @property
    def base_url(self) -> str:
        """Returns the JSON endpoint URL for the subreddit."""
        return (
            f"https://www.reddit.com/r/{self.subreddit}"
            f"/{self.sort_type}.json"
        )

    def build_params(self) -> dict:
        """Builds the parameter object to pass into the request."""
        params = {"limit": self.page_limit}
        if self.sort_type in self.TIME_FILTERED_SORTS:
            params["t"] = self.time_filter
        return params

    def _request_with_retries(
        self,
        url: str,
        params: dict,
        retries: int = 5,
        context: str = "",
    ) -> requests.Response | None:
        """Sends a GET request with retry logic for rate limits and server errors."""
        for attempt in range(retries):
            response = requests.get(
                url, params=params, headers=self.header
            )

            if response.ok:
                return response

            if response.status_code == 429:
                wait = int(response.headers.get(
                    "Retry-After", 5 * (attempt + 1)
                ))
                logger.warning(
                    "Rate limited%s, waiting %s seconds", context, wait
                )
                time.sleep(wait)

            elif response.status_code >= 500:
                logger.warning(
                    "Attempt %s%s failed: %s",
                    attempt + 1, context, response.status_code,
                )
                time.sleep(2 + attempt)

            else:
                logger.error(
                    "Unexpected status %s%s", response.status_code, context
                )
                break

        logger.error("All %s attempts failed%s", retries, context)
        return None

    def get_post_data(self, retries: int = 5) -> dict:
        """Returns the post data for the object's subreddit."""
        context = f" for subreddit: {self.subreddit}"
        response = self._request_with_retries(
            self.base_url, self.build_params(), retries, context
        )
        return response.json() if response else {}

    def get_comment_data(
        self,
        post_id: str,
        sort: str = "top",
        limit: int = 25,
        min_upvotes: int = 1,
        retries: int = 5,
    ) -> list[dict]:
        """Returns top-level comments for a given post, filtered by minimum upvotes."""
        url = (
            f"https://www.reddit.com/r/{self.subreddit}"
            f"/comments/{post_id}.json"
        )
        params = {"sort": sort, "limit": limit}
        context = f" fetching comments for post {post_id}"

        response = self._request_with_retries(url, params, retries, context)
        if not response:
            return []

        # Reddit returns [post_listing, comment_listing]
        comments_listing = response.json()[1]
        return [
            child["data"]
            for child in comments_listing["data"]["children"]
            if child["kind"] == "t1" and child["data"]["ups"] >= min_upvotes
        ]

    def enrich_posts_with_comments(
        self,
        posts: list[dict],
        comment_sort: str = "top",
        comment_limit: int = 25,
        min_upvotes: int = 5,
        delay: float = 3.0,
    ) -> list[dict]:
        """Attaches comment data to each post under a 'comments' key."""
        for post in posts:
            post_id = post["data"]["id"]
            logger.info(
                "Fetching comments for post %s in r/%s",
                post_id, self.subreddit,
            )
            post["data"]["comments"] = self.get_comment_data(
                post_id,
                sort=comment_sort,
                limit=comment_limit,
                min_upvotes=min_upvotes,
            )
            time.sleep(delay)

        return posts


def extract_main(
    subreddits: list[str],
    include_comments: bool = False,
) -> list[dict]:
    """Main function to run the functionality of the extract script."""
    results = []

    for subreddit in subreddits:
        sub = RedditExtractor(subreddit)
        result = sub.get_post_data()

        if not result:
            logger.warning("No data returned for r/%s, skipping", subreddit)
            continue

        post_data = result.get("data", {}).get("children", [])

        if include_comments:
            post_data = sub.enrich_posts_with_comments(post_data)

        results += post_data
        time.sleep(1)

    return results


if __name__ == "__main__":
    subreddits = [
        "trading", "stocks", "investing", "stockmarket",
        "valueinvesting", "options", "algotrading", "semiconductors",
        "artificialinteligence", "cloudcomputing", "hardware",
        "wallstreetbets",
    ]
    results = extract_main(subreddits, include_comments=False)

    # To do
    # Figure out which json results to keep
    # Get comment data for each relevant subreddit. This will be very intensive so I'm wondering about the logic.
    # It think it should only be for relevant posts which mention the stock themselves and also there needs to be a
    # limit on the number of comments checked as well, maybe related to the relevance to the mentioned stock and or
    # the number of comments already checked that include the data.
    # Get comments logic
    # Design choice: Should the comments of a post be directly linked to that post for the sentiment and related stocks etc...
    # Transform script
    # Including post comments is too challenging as you need to do 25x (for each post) as many requests as the standard
    # and will be frequently rate limited unless using a reddit authenticated API
