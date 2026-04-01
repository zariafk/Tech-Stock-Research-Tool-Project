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
    get_company_summary,
    render_summary_analytics,
    TIME_OPTIONS
)
from chatbot import render_chatbot

load_dotenv()

SECRETS_REPO = os.environ["SECRETS_REPO_NAME"]


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

# 2ND TICKER COMPARISON


def add_ticker_label(dataframe: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Add ticker label to a dataframe."""
    if dataframe.empty:
        return pd.DataFrame()

    dataframe = dataframe.copy()
    dataframe["ticker"] = ticker
    return dataframe


def combine_ticker_data(
    primary_df: pd.DataFrame,
    compare_df: pd.DataFrame,
    primary_ticker: str,
    compare_ticker: str | None,
) -> pd.DataFrame:
    """Combine primary and optional comparison ticker data."""
    frames = [add_ticker_label(primary_df, primary_ticker)]

    if compare_ticker and not compare_df.empty:
        frames.append(add_ticker_label(compare_df, compare_ticker))

    frames = [frame for frame in frames if not frame.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# Dashboard helpers
# ---------------------------------------------------------------------------
def compute_cutoff_date(time_days: int | None):
    """Convert a number-of-days value into a date cutoff, or None for all data."""
    if time_days is None:
        return None
    return (pd.Timestamp.today().normalize() - pd.Timedelta(days=time_days)).date()


def fetch_primary_datasets(
    stock_id: int,
    cutoff_date,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fetch all primary datasets for the selected stock and time range."""
    latest, history = fetch_market_data(conn, stock_id, cutoff_date)
    news = fetch_news_signals(conn, stock_id, cutoff_date)
    social = fetch_social_signals(conn, stock_id, cutoff_date)
    extended_social = fetch_extended_social(conn, stock_id, cutoff_date)
    return latest, history, news, social, extended_social


def fetch_comparison_datasets(
    compare_input: str,
    cutoff_date,
) -> tuple[str | None, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fetch datasets for an optional comparison ticker. Returns empty frames if not found."""
    empty = pd.DataFrame()

    if not compare_input.strip():
        return None, empty, empty, empty, empty

    compare_result = fetch_stock_by_ticker_or_name(conn, compare_input.strip())

    if not compare_result:
        st.warning("Comparison ticker not found.")
        return None, empty, empty, empty, empty

    compare_stock_id, compare_ticker, _ = compare_result
    _, compare_history = fetch_market_data(conn, compare_stock_id, cutoff_date)
    compare_extended_social = fetch_extended_social(
        conn, compare_stock_id, cutoff_date)
    compare_social = fetch_social_signals(conn, compare_stock_id, cutoff_date)
    compare_news = fetch_news_signals(conn, compare_stock_id, cutoff_date)
    return compare_ticker, compare_history, compare_extended_social, compare_social, compare_news


def build_combined_datasets(
    primary_ticker: str,
    compare_ticker: str | None,
    history: pd.DataFrame,
    extended_social: pd.DataFrame,
    social: pd.DataFrame,
    news: pd.DataFrame,
    compare_history: pd.DataFrame,
    compare_extended_social: pd.DataFrame,
    compare_social: pd.DataFrame,
    compare_news: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Merge primary and comparison dataframes into combined datasets."""
    combined_history = combine_ticker_data(
        history, compare_history, primary_ticker, compare_ticker)
    combined_extended = combine_ticker_data(
        extended_social, compare_extended_social, primary_ticker, compare_ticker)
    combined_social = combine_ticker_data(
        social, compare_social, primary_ticker, compare_ticker)
    combined_news = combine_ticker_data(
        news, compare_news, primary_ticker, compare_ticker)
    return combined_history, combined_extended, combined_social, combined_news


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
def dashboard():
    """Render the full summary dashboard for a user-searched stock."""
    with st.container(border=True):
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

        time_label = st.radio(
            "Time Range",
            list(TIME_OPTIONS.keys()),
            horizontal=True,
            key=f"trends_time_range_{ticker}",
        )
        cutoff_date = compute_cutoff_date(TIME_OPTIONS[time_label])

    st.divider()

    summary_title, summary_info = st.columns([6, 1])
    with summary_title:
        st.markdown("#### 📊 Company Summary")
    with summary_info:
        with st.popover("ℹ️"):
            st.markdown(
                "**Company Summary**\n\n"
                "AI-generated overview powered by our RAG service. "
                "Combines recent price movements, news headlines, and "
                "Reddit sentiment into a plain-English briefing for the "
                "selected stock."
            )
    with st.expander("View Summary", expanded=True):
        with st.spinner("Generating summary..."):
            summary = get_company_summary(ticker, company_name)
        st.write(summary)

    st.divider()

    latest, history, news, social, extended_social = fetch_primary_datasets(
        stock_id, cutoff_date)

    market_title, market_info = st.columns([6, 1])
    with market_title:
        st.header(f"Market Data — {ticker} ({company_name})")
    with market_info:
        with st.popover("ℹ️"):
            st.markdown(
                "**Market Data**\n\n"
                "Live price metrics for the selected stock within the "
                "chosen time range:\n\n"
                "- **Current Price** — latest closing price with change vs period start.\n"
                "- **Range** — highest and lowest prices in the period.\n"
                "- **Volume** — total shares traded in the period.\n"
                "- **Trend** — direction based on the last 5 closing prices."
            )
    render_market_section(latest, history, time_label)
    st.divider()

    compare_input = st.text_input(
        "Compare with another ticker (optional)",
        placeholder="e.g. MSFT",
        key=f"compare_ticker_{ticker}",
    )

    compare_ticker, compare_history, compare_extended_social, compare_social, compare_news = (
        fetch_comparison_datasets(compare_input, cutoff_date)
    )

    combined_history, combined_extended, combined_social, combined_news = build_combined_datasets(
        ticker, compare_ticker,
        history, extended_social, social, news,
        compare_history, compare_extended_social, compare_social, compare_news,
    )

    render_summary_analytics(
        combined_history, combined_extended, combined_social, combined_news)

    news_title, news_info = st.columns([6, 1])
    with news_title:
        st.header("News & Market Signals")
    with news_info:
        with st.popover("ℹ️"):
            st.markdown(
                "**News & Market Signals**\n\n"
                "Aggregated RSS news articles scored by our sentiment "
                "analysis pipeline:\n\n"
                "- **News Sentiment** — average sentiment across all articles (-1 to +1).\n"
                "- **Articles Tracked** — total number of articles in the period.\n"
                "- **Recent Coverage** — scrollable cards showing each article with "
                "its sentiment score, relevance, confidence, and AI-generated take."
            )
    render_news_section(news)
    st.divider()

    social_title, social_info = st.columns([6, 1])
    with social_title:
        st.header("Community Sentiment")
    with social_info:
        with st.popover("ℹ️"):
            st.markdown(
                "**Community Sentiment**\n\n"
                "Reddit discussion analysis for the selected stock:\n\n"
                "- **Reddit Sentiment** — average sentiment of tracked posts.\n"
                "- **Total Comments** — combined comment count across all posts.\n"
                "- **Top Discussions** — expandable cards for the most active posts.\n"
                "- **Source Divergence** — compares news vs Reddit sentiment. "
                "High divergence can signal conflicting institutional vs retail views."
            )
    render_social_section(social)
    render_divergence_section(news, social)
    st.divider()

    st.caption(
        "_Dashboard updated with live data from RDS. Refresh to see latest signals._")


if __name__ == "__main__":
    dashboard()
