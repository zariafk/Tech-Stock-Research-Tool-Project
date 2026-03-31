"""
dashboard.py

Summary dashboard — stock deep-dive with market data, news, and social signals.
"""

import json
import os

import boto3
import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

from .queries import (
    STOCK_SEARCH_QUERY,
    MARKET_LATEST_QUERY,
    MARKET_HISTORY_QUERY,
    NEWS_SIGNALS_QUERY,
    SOCIAL_SIGNALS_QUERY,
    EXTENDED_SOCIAL_QUERY,
    FULL_MARKET_HISTORY_QUERY,
)
from .helpers import (
    render_market_section,
    render_news_section,
    render_social_section,
    render_divergence_section,
    render_summary_analytics,
    TIME_OPTIONS
)

load_dotenv()

SECRETS_REPO = "c22-trade-research-tool-secrets"


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
def get_secret(secret_name: str, region: str = "eu-west-2") -> dict:
    """Retrieves a secret from AWS Secrets Manager and returns it as a dict."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_connection():
    """Connect to RDS PostgreSQL. Uses env vars for local dev, Secrets Manager for prod."""
    if os.environ.get("DB_HOST"):
        return psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", 5432),
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        )

    secret = get_secret(SECRETS_REPO)
    return psycopg2.connect(
        host=secret["host"],
        port=int(secret["port"]),
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        sslmode="require",
    )


if "conn" not in st.session_state:
    st.session_state.conn = None

if st.session_state.conn is None:
    try:
        st.session_state.conn = get_connection()
        st.sidebar.success("Connected to RDS ✓")
    except Exception as e:
        st.sidebar.error("Could not connect to database.")
        st.sidebar.caption(str(e))

conn = st.session_state.conn


# ---------------------------------------------------------------------------
# Cached queries (20-minute TTL)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=1200, show_spinner="Searching for stock...")
def fetch_stock_by_ticker_or_name(_conn, search_term: str) -> tuple | None:
    """Search for stock by ticker or name. Returns (stock_id, ticker, stock_name) or None."""
    search_lower = search_term.lower()
    cursor = _conn.cursor()
    cursor.execute(STOCK_SEARCH_QUERY, (search_lower, f"%{search_lower}%"))
    result = cursor.fetchone()
    cursor.close()
    return result


@st.cache_data(ttl=1200, show_spinner="Fetching market data...")
def fetch_market_data(_conn, stock_id: int, cutoff_date) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch latest market data and filtered historical trend."""
    latest = pd.read_sql(MARKET_LATEST_QUERY, _conn, params=(stock_id,))
    history = pd.read_sql(
        MARKET_HISTORY_QUERY,
        _conn,
        params=(stock_id, cutoff_date, cutoff_date)
    )
    return latest, history


@st.cache_data(ttl=1200, show_spinner="Fetching news signals...")
def fetch_news_signals(_conn, stock_id: int, cutoff_date) -> pd.DataFrame:
    """Fetch RSS news articles with sentiment and relevance scores."""
    return pd.read_sql(
        NEWS_SIGNALS_QUERY,
        _conn,
        params=(stock_id, cutoff_date, cutoff_date)
    )


@st.cache_data(ttl=1200, show_spinner="Fetching social signals...")
def fetch_social_signals(_conn, stock_id: int, cutoff_date) -> pd.DataFrame:
    """Fetch Reddit posts with sentiment and relevance scores."""
    return pd.read_sql(
        SOCIAL_SIGNALS_QUERY,
        _conn,
        params=(stock_id, cutoff_date, cutoff_date)
    )


@st.cache_data(ttl=1200, show_spinner="Fetching extended social data...")
def fetch_extended_social(_conn, stock_id: int, cutoff_date) -> pd.DataFrame:
    """Fetch Reddit posts with full engagement data for chart rendering."""
    return pd.read_sql(
        EXTENDED_SOCIAL_QUERY,
        _conn,
        params=(stock_id, cutoff_date, cutoff_date)
    )


@st.cache_data(ttl=1200, show_spinner="Fetching full market history...")
def fetch_full_market_history(_conn, stock_id: int, cutoff_date) -> pd.DataFrame:
    """Fetch filtered price history for technical indicator computation."""
    return pd.read_sql(
        FULL_MARKET_HISTORY_QUERY,
        _conn,
        params=(stock_id, cutoff_date, cutoff_date)
    )
# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


def dashboard():
    """Render the full summary dashboard for a user-searched stock."""
    st.caption(
        "Consolidated view of market data, news signals, and community sentiment for specific stocks.")
    st.divider()

    col1, col2 = st.columns([3, 1])
    with col1:
        search_input = st.text_input(
            "Search by ticker or company name",
            placeholder="e.g., AAPL or Apple",
            key="stock_search",
        )
    with col2:
        st.write("")
        search_btn = st.button("Search", use_container_width=True)

    if not (search_btn or search_input):
        return

    if not search_input:
        st.warning("Please enter a stock ticker or company name.")
        return

    stock_result = fetch_stock_by_ticker_or_name(conn, search_input)
    if not stock_result:
        st.error("Stock not found. Please check the ticker or company name.")
        return

    stock_id, ticker, company_name = stock_result
    st.divider()

    time_label = st.radio(
        "Time Range",
        list(TIME_OPTIONS.keys()),
        horizontal=True,
        key=f"trends_time_range_{ticker}",
    )
    time_days = TIME_OPTIONS[time_label]

    if time_days is None:
        cutoff_date = None
    else:
        cutoff_date = (
            pd.Timestamp.today().normalize() - pd.Timedelta(days=time_days)
        ).date()

    st.divider()

    latest, history = fetch_market_data(conn, stock_id, cutoff_date)
    news = fetch_news_signals(conn, stock_id, cutoff_date)
    social = fetch_social_signals(conn, stock_id, cutoff_date)
    extended_social = fetch_extended_social(conn, stock_id, cutoff_date)

    st.header(f"Market Data — {ticker} ({company_name})")
    render_market_section(latest, history)
    st.divider()

    render_summary_analytics(history, extended_social, social, news)

    st.header("News & Market Signals")
    render_news_section(news)
    st.divider()

    st.header("Community Sentiment")
    render_social_section(social)
    render_divergence_section(news, social)
    st.divider()

    st.caption(
        "_Dashboard updated with live data from RDS. Refresh to see latest signals._")


if __name__ == "__main__":
    dashboard()
