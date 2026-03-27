"""
This module contains functions for ingesting and normalizing stock data from various 
sources, including Alpaca, Reddit, and RSS feeds. 
"""

import json


def load_data(file_path: str) -> list:
    """Load stock data from a JSON file."""
    with open(file_path, "r") as f:
        return json.load(f)


def get_input_data(data_path: str = None, data: list = None) -> list:
    """Get input data either from a file path or directly from a provided data object."""
    if data is not None:
        return data

    if data_path is not None:
        return load_data(data_path)

    raise ValueError("Either data_path or data must be provided.")


def convert_to_documents(data: list, source: str) -> list:
    """Convert raw stock data into a list of documents with text and metadata."""
    documents = []

    for record in data:
        # if the record is already RAG-ready - only includes RSS
        if "text" in record and "metadata" in record:
            documents.append(record)
            continue

        if source == "alpaca":
            if "latest_time" in record:
                document = normalize_alpaca_live_record(record)
            elif "bar_date" in record:
                document = normalize_alpaca_historical_record(record)
            else:
                continue

        elif source == "reddit":
            document = normalize_reddit_record(record)

        else:
            continue

        if isinstance(document, list):
            documents.extend(document)
        elif document is not None:
            documents.append(document)

    return documents


def normalize_alpaca_live_record(record) -> dict | None:
    ticker = record.get("ticker")
    timestamp = record.get("latest_time")

    if not ticker or not timestamp:
        return None

    text = f"{ticker} at {timestamp}. "

    metrics = ["open", "high", "low", "close", "volume"]
    for metric in metrics:
        if record.get(metric) is not None:
            text += f"{metric.capitalize()} {record[metric]}. "

    return {
        "id": f"alpaca_live_{ticker}_{timestamp}",
        "text": text.strip(),
        "metadata": {
            "source": "alpaca",
            "ticker": ticker,
            "timestamp": timestamp,
            "doc_type": "live_bar"
        }
    }


def normalize_alpaca_historical_record(record: dict) -> dict | None:
    """Normalize Alpaca stock summary data into a document format."""
    ticker = record.get("ticker")

    date = record.get("bar_date")

    if not ticker or not date:
        return None

    stock_name = ticker

    text = f"{stock_name} ({ticker}) on {date}. "

    metrics = ["open", "high", "low", "close", "volume"]

    for metric in metrics:
        if record.get(metric) is not None:
            text += f"{metric.capitalize()} {record[metric]}. "

    if record.get("open") is not None and record.get("close") is not None:
        change_pct = (
            (record["close"] - record["open"]) / record["open"]) * 100
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


def normalize_reddit_record(record: dict) -> list[dict]:
    """Normalize a Reddit post into one document per ticker."""
    title = record.get("title", "")
    body = record.get("contents", "")
    flair = record.get("flair")
    created_at = record.get("created_at")
    post_id = record.get("post_id")
    tickers = record.get("tickers", [])

    if not title and not body:
        return []

    base_text = f"{title}. {body}".strip()

    if flair:
        base_text += f" Flair: {flair}"

    documents = []

    for ticker_info in tickers:
        ticker = ticker_info.get("ticker")
        if not ticker:
            continue

        doc_text = base_text
        analysis = ticker_info.get("analysis")

        if analysis:
            doc_text += f" Analysis: {analysis}"

        documents.append({
            "id": f"reddit_{post_id}_{ticker}",
            "text": doc_text,
            "metadata": {
                "source": "reddit",
                "ticker": ticker,
                "timestamp": created_at,
                "url": record.get("url"),
                "permalink": record.get("permalink"),
                "post_id": record.get("post_id"),
                "author": record.get("author"),
                "subreddit_id": record.get("subreddit_id"),
                "subreddit_name": record.get("subreddit_name"),
                "subreddit_subscribers": record.get("subreddit_subscribers"),
                "flair": flair,
                "score": record.get("score"),
                "ups": record.get("ups"),
                "upvote_ratio": record.get("upvote_ratio"),
                "num_comments": record.get("num_comments"),
                "relevance_score": ticker_info.get("relevance_score"),
                "sentiment": ticker_info.get("sentiment"),
            }
        })

    return documents
