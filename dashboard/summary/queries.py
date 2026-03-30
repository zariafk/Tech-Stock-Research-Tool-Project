import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    """Establish connection to PostgreSQL RDS database."""
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", "5432")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME"),
            sslmode="require"
        )
    except psycopg2.DatabaseError as err:
        raise psycopg2.DatabaseError(
            "Failed to connect to database. Check environment variables."
        ) from err


def get_stock_by_ticker_or_name(search_term):
    """Search for stock by ticker or name. Returns (stock_id, ticker, stock_name) or None."""
    conn = get_db_connection()
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
        SELECT ra.sentiment_score, ra.relevance_score, ra.analysis,
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
        SELECT ra.sentiment_score, ra.relevance_score, ra.analysis,
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
        SELECT ra.sentiment_score, ra.relevance_score, ra.analysis,
               rp.post_id, rp.title, rp.contents, rp.score, rp.ups,
               rp.upvote_ratio, rp.num_comments, rp.created_at
        FROM reddit_analysis ra
        JOIN reddit_post rp ON ra.story_id = rp.post_id
        WHERE ra.stock_id = %s
        ORDER BY rp.created_at DESC LIMIT 200
    """, conn, params=(stock_id,))
    conn.close()
    return social
