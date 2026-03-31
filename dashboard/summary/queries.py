import requests
import os
import json

import psycopg2
import pandas as pd
import boto3


SECRETS_REPO = os.getenv("SECRETS_REPO")
RAG_API_URL = os.getenv("RAG_API_URL")


def get_secret(secret_name: str, region: str = "eu-west-2") -> dict:
    """Retrieves a secret from AWS Secrets Manager and returns it as a dict."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_db_connection():
    """Establish connection to PostgreSQL RDS database."""
    try:
        secrets = get_secret(SECRETS_REPO)

        conn = psycopg2.connect(
            host=secrets["host"],
            port=int(secrets["port"]),
            user=secrets["username"],
            password=secrets["password"],
            dbname=secrets["dbname"],
            sslmode="require"
        )
        return conn

    except Exception as err:
        raise Exception(f"Error connecting to database: {err}") from err


def get_stock_by_ticker_or_name(search_term):
    """Search for stock by ticker or name. Returns (stock_id, ticker, stock_name) or None."""
    print("get_stock_by_ticker_or_name called")
    conn = get_db_connection()
    print("conn:", conn)
    cursor = conn.cursor()
    search_lower = search_term.lower()

    cursor.execute("""
        SELECT stock_id, ticker, stock_name FROM stock
        WHERE LOWER(ticker) = %s OR LOWER(stock_name) LIKE %s
        LIMIT 1
    """, (search_lower, f"%{search_lower}%"))

    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result


def get_market_data(stock_id):
    """Fetch latest market data and 30-day historical trend. Returns (latest_df, history_df)."""
    conn = get_db_connection()

    latest = pd.read_sql_query("""
        SELECT close, open, high, low, volume, latest_time FROM alpaca_live
        WHERE stock_id = %s
        ORDER BY latest_time DESC LIMIT 1
    """, conn, params=(stock_id,))

    history = pd.read_sql_query("""
        SELECT bar_date, open, high, low, close, volume FROM alpaca_history
        WHERE stock_id = %s
        ORDER BY bar_date DESC LIMIT 30
    """, conn, params=(stock_id,))

    conn.close()
    return latest, history


def get_news_signals(stock_id):
    """Fetch RSS news articles with sentiment and relevance scores."""
    conn = get_db_connection()
    news = pd.read_sql_query("""
        SELECT ra.sentiment_score, ra.relevance_score, ra.confidence, ra.analysis,
               rss.title, rss.summary, rss.published_date, rss.source
        FROM rss_analysis ra
        JOIN rss_article rss ON ra.story_id = rss.story_id
        WHERE ra.stock_id = %s
        ORDER BY rss.published_date DESC LIMIT 20
    """, conn, params=(stock_id,))
    conn.close()
    return news


def get_social_signals(stock_id):
    """Fetch Reddit posts with sentiment and relevance scores."""
    conn = get_db_connection()
    social = pd.read_sql_query("""
        SELECT ra.sentiment_score, ra.relevance_score, ra.confidence, ra.analysis,
               rp.title, rp.score, rp.num_comments, rp.created_at, rp.url
        FROM reddit_analysis ra
        JOIN reddit_post rp ON ra.story_id = rp.post_id
        WHERE ra.stock_id = %s
        ORDER BY rp.created_at DESC LIMIT 20
    """, conn, params=(stock_id,))
    conn.close()
    return social


def get_extended_social(stock_id: int) -> pd.DataFrame:
    """Fetch Reddit posts with full engagement data for chart rendering (200 most recent)."""
    conn = get_db_connection()
    social = pd.read_sql_query("""
        SELECT ra.sentiment_score, ra.relevance_score, ra.confidence, ra.analysis,
               rp.post_id, rp.title, rp.contents, rp.score, rp.ups,
               rp.upvote_ratio, rp.num_comments, rp.created_at
        FROM reddit_analysis ra
        JOIN reddit_post rp ON ra.story_id = rp.post_id
        WHERE ra.stock_id = %s
        ORDER BY rp.created_at DESC LIMIT 200
    """, conn, params=(stock_id,))
    conn.close()
    return social


def get_full_market_history(stock_id: int) -> pd.DataFrame:
    """Fetch up to 365 days of price history for technical indicator computation."""
    conn = get_db_connection()
    history = pd.read_sql_query("""
        SELECT bar_date, open, high, low, close, volume, trade_count, vwap
        FROM alpaca_history
        WHERE stock_id = %s
        ORDER BY bar_date ASC LIMIT 365
    """, conn, params=(stock_id,))
    conn.close()
    return history


def get_company_summary(ticker: str, company_name: str) -> str:
    payload = {
        "question": f"Generate a summary for {company_name} ({ticker}) including recent price context, news, and sentiment.",
        "ticker": ticker
    }

    try:
        response = requests.post(RAG_API_URL, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("answer", "No summary returned.")
    except requests.RequestException as e:
        return f"Error retrieving summary: {e}"
