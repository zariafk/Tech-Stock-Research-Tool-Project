"""Reddit data extract script"""


import requests


def load_tickers(file_path: str = "tickers.csv") -> list[str]:
    """Loads in the tickers for the stocks of interest."""
    with open(file_path, "r") as f:
        return f.read().splitlines()


def get_json(url: str, subreddit: str) -> dict:
    """Gets the json for the given subreddits posts."""
    headers = {"User-Agent": "script:v1.0 (by /u/samjfg)"}
    return requests.get(url % subreddit).json()


if __name__ == "__main__":
    # To do
    # Figure out which json results to keep
    # Figure out whether all subreddits are needed
    # Figure out how to query all relevant subreddits without hitting limit rates
    subreddits = ["trading", "stocks", "investing", "stockmarket", "valueinvesting", "options", "algotrading",
                  "semiconductors", "artificialintelligence", "cloudcomputing", "hardware", "wallstreetbets"]
    url = "https://www.reddit.com/r/%s/.json"

    result = get_json(url, subreddits[0])
    print(result)
    print(result.get("error"))
    print(result.get("message"))
    print(result.get("kind"))
    print(result.get("data"))
    print(result.keys())
    print(type(result))
