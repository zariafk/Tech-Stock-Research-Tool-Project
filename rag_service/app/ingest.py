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


def validate_alpaca_record(record, required_metrics):
    """Validate that a record has the required fields and at least one metric."""

    # ticker and timestamp are required
    if (("ticker" not in record)
        or ("timestamp" not in record)
        # at least one of the required metrics must be present
            or (not any(metric in record for metric in required_metrics))):
        return False
    return True


def convert_to_documents(data, source):
    """Convert raw stock data into a list of documents with text and metadata."""
    documents = []

    for record in data:
        if source == "alpaca":
            text = normalise_alpaca_record(record)

            if text == "":
                continue

            documents.append({
                "text": text,
                "metadata": {
                    "source": "alpaca",
                    "ticker": record["ticker"],
                    "timestamp": record["timestamp"]
                }
            })

        elif source == "rss":
            document = normalize_rss_record(record)

            if document is not None:
                documents.append(document)

    return documents


def normalise_alpaca_record(record) -> str:
    """Convert raw Alpaca stock data into a standardized document format."""
    required_metrics = ["open", "high", "low", "close", "volume"]

    if not validate_alpaca_record(record, required_metrics):
        return ""

    text = f"Stock {record['ticker']} at {record['timestamp']}"

    for required_metric in required_metrics:
        if required_metric in record:
            text += f", {required_metric} {record[required_metric]}"

    text += "."
    return text


def normalize_rss_record(record):
    """Convert raw RSS feed data into a standardized document format."""
    text = record.get("text", "").strip()
    metadata = record.get("metadata", {})

    if not text or not metadata:
        return None

    return {
        "text": text,
        "metadata": {
            "source": metadata.get("source", "rss"),
            "ticker": metadata.get("ticker"),
            "timestamp": metadata.get("published_date"),
            "link": metadata.get("link"),
            "relevance_score": metadata.get("relevance_score"),
            "sentiment": metadata.get("sentiment"),
        }
    }


def normalize_reddit_record(record):
    title = record.get("title", "")
    body = record.get("selftext", "")
    score = record.get("score")
    num_comments = record.get("num_comments")
    created_utc = record.get("created_utc")
    url = record.get("url")
    subreddit_id = record.get("subreddit_id")

    if not title:
        return None

    timestamp = (
        datetime.utcfromtimestamp(created_utc).isoformat()
        if created_utc else None
    )

    text = f"{title}. {body}".strip()

    return {
        "text": text,
        "metadata": {
            "source": "reddit",
            "timestamp": timestamp,
            "url": url,
            "score": score,
            "num_comments": num_comments,
            "subreddit_id": subreddit_id
        }
    }
