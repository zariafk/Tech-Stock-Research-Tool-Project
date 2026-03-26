from datetime import datetime
import json


def load_data(file_path):
    """Load stock data from a JSON file."""
    with open(file_path, "r") as f:
        return json.load(f)


def get_input_data(data_path=None, data=None):
    """Get input data either from a file path or directly from a provided data object."""
    if data is not None:
        return data

    if data_path is not None:
        return load_data(data_path)

    raise ValueError("Either data_path or data must be provided.")


def convert_to_documents(data, source):
    """Convert raw stock data into a list of documents with text and metadata."""
    documents = []

    for record in data:
        # if the record is already RAG-ready - only includes RSS for now
        if "text" in record and "metadata" in record:
            documents.append(record)
            continue

        if source == "alpaca":
            document = normalize_alpaca_summary(record)

        elif source == "reddit":
            document = normalize_reddit_record(record)

        else:
            continue

        if document is not None:
            documents.append(document)

    return documents


def normalize_alpaca_summary(record) -> dict | None:
    """Normalize Alpaca stock summary data into a document format."""
    alpaca = record.get("alpaca")
    stock = record.get("stock")

    if not alpaca or not stock:
        return None

    ticker = stock.get("ticker")
    stock_name = stock.get("stock_name")

    date = alpaca.get("date")  # or derive from timestamp
    open_price = alpaca.get("open")
    high = alpaca.get("high")
    low = alpaca.get("low")
    close = alpaca.get("close")
    volume = alpaca.get("volume")

    if not ticker or not date or close is None:
        return None

    # basic derived metric
    change_pct = None
    if open_price and close:
        change_pct = ((close - open_price) / open_price) * 100

    # Build text (this is what gets embedded)
    text = f"{stock_name} ({ticker}) on {date}. "

    if open_price is not None:
        text += f"Open {open_price}. "

    if high is not None:
        text += f"High {high}. "

    if low is not None:
        text += f"Low {low}. "

    if close is not None:
        text += f"Close {close}. "

    if volume is not None:
        text += f"Volume {volume}. "

    if open_price is not None and close is not None:
        change_pct = ((close - open_price) / open_price) * 100
        text += f"Change {round(change_pct, 2)} percent. "

    return {
        "id": f"alpaca_{ticker}_{date}",
        "text": text.strip(),
        "metadata": {
            "source": "alpaca",
            "ticker": ticker,
            "stock_name": stock_name,
            "date": date,
            "doc_type": "daily_summary"
        }
    }


def normalize_reddit_record(record) -> dict | None:
    """Normalize a Reddit post record into a document format."""
    post = record.get("reddit_post")
    stock = record.get("stock")

    if not post or not stock:
        return None

    ticker = stock.get("ticker")
    stock_name = stock.get("stock_name")

    title = post.get("title", "")
    body = post.get("contents", "")
    flair = post.get("flair")
    score = post.get("score")
    ups = post.get("ups")
    upvote_ratio = post.get("upvote_ratio")
    num_comments = post.get("num_comments")
    created_at = post.get("created_at")
    url = post.get("url")
    subreddit_id = post.get("subreddit_id")

    if not title and not body:
        return None

    text = f"{title}. {body}".strip()

    if flair:
        text += f" Flair: {flair}"

    return {
        "text": text,
        "metadata": {
            "source": "reddit",
            "ticker": ticker,
            "stock_name": stock_name,
            "timestamp": created_at,
            "url": url,
            "subreddit_id": subreddit_id,
            "flair": flair,
            "score": score,
            "ups": ups,
            "upvote_ratio": upvote_ratio,
            "num_comments": num_comments
        }
    }
