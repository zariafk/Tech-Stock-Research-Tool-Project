"""Reddit data extract script"""


import time
import logging

import requests
import pandas as pd

# from ../logger import make_logger

# logger = make_logger()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_mappings(file_path: str = "../company_mapping.csv") -> list[str]:
    """Loads in the tickers for the stocks of interest."""
    return pd.read_csv(file_path)


class RedditExtractor:
    """
    Each object of this class represents a unique subreddit,
    which can be queried for data containing recent posts.
    """

    VALID_SORTS = {"hot", "new", "top", "rising", "controversial"}
    TIME_FILTERED_SORTS = {"top", "controversial"}

    def __init__(self,
                 subreddit: str,
                 sort_type: str = "hot",
                 page_limit: int = 25,
                 time_filter: str = "day",
                 header: dict | None = None
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
        return f"https://www.reddit.com/r/{self.subreddit}/{self.sort_type}.json"

    def build_params(self) -> dict:
        """Builds the parameter object to pass into the request."""
        params = {"limit": self.page_limit}
        if self.sort_type in self.TIME_FILTERED_SORTS:
            params["t"] = self.time_filter
        return params

    def get_post_data(self, retries: int = 5) -> dict:
        """Returns the post data for the object's subreddit."""
        for attempt in range(retries):
            response = requests.get(
                self.base_url,
                params=self.build_params(),
                headers=self.header
            )

            if response.ok is True:
                return response.json()

            elif response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 2))
                logger.warning(
                    "Rate limited for subreddit: %s, waiting %s seconds", self.subreddit, wait)
                time.sleep(wait)

            elif str(response.status_code).startswith("5"):
                logger.warning("Attempt %s for subreddit: %s failed: %s",
                               attempt + 1, self.subreddit, response.status_code)
                time.sleep(2 + attempt)

        logger.error("All %s attempts failed for %s", retries, self.subreddit)
        return {}

    def get_comment_data(self,
                         post_id: str,
                         sort: str = "top",
                         limit: int = 25,
                         min_upvotes: int = 1) -> list[dict]:
        """Returns the top comments for a given post, filtered by minimum upvotes."""
        url = f"https://www.reddit.com/r/{self.subreddit}/comments/{post_id}.json"
        params = {"sort": sort, "limit": limit}

        for attempt in range(5):
            response = requests.get(url, params=params, headers=self.header)

            if response.ok:
                # Reddit returns [post_listing, comment_listing]
                comments_listing = response.json()[1]
                comments = [
                    child["data"]
                    for child in comments_listing["data"]["children"]
                    if child["kind"] == "t1" and child["data"]["ups"] >= min_upvotes
                ]
                return comments

            elif response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 2))
                logger.warning(
                    "Rate limited fetching comments for post %s, waiting %s seconds", post_id, wait)
                time.sleep(wait)

            elif str(response.status_code).startswith("5"):
                logger.warning("Attempt %s for comments on post %s failed: %s",
                               attempt + 1, post_id, response.status_code)
                time.sleep(2 + attempt)

        logger.error(
            "All attempts failed fetching comments for post %s", post_id)
        return []


def extract_main(subreddits: list[str]) -> dict:
    """Main function to run the functionality of the extract script."""
    results = []
    # Iterates through each subreddit to gather all results
    for subreddit in subreddits:
        sub = RedditExtractor(subreddit)
        result = sub.get_post_data()
        post_data = result.get("data").get("children")
        results.append(result)
        time.sleep(1)
        # Pauses for 1 second to avoid rate limiting

    return results


def comment_testing():
    sub = RedditExtractor("wallstreetbets")
    result = sub.get_post_data()
    print(result)
    print(type(result))
    # print(result.keys())


if __name__ == "__main__":
    # To do
    # Figure out which json results to keep
    # Get comment data for each relevant subreddit. This will be very intensive so I'm wondering about the logic.
    # It think it should only be for relevant posts which mention the stock themselves and also there needs to be a
    # limit on the number of comments checked as well, maybe related to the relevance to the mentioned stock and or
    # the number of comments already checked that include the data.
    # Get comments logic
    # Transform script

    # mappings = load_mappings()
    # tickers = mappings["symbol"].tolist()
    # print(tickers)
    subreddits = ["trading", "stocks", "investing", "stockmarket", "valueinvesting", "options", "algotrading",
                  "semiconductors", "artificialinteligence", "cloudcomputing", "hardware", "wallstreetbets"]

    results = extract_main(subreddits)
    print([result.get("data").get("id")for result in results])

    # comment_testing()
    # print(results)
