"""
app.py

Streamlit dashboard for the Tech Stock Research Tool.
Connects to PostgreSQL RDS and displays data across three tabs.

Run:
    streamlit run app.py
"""

import json
import os

import boto3
import psycopg2
import streamlit as st
import pandas as pd
import altair as alt
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Tech Stock Research",
    page_icon="📈",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
def get_secret(secret_name: str, region: str = "eu-west-2") -> dict:
    """Retrieves a secret from AWS Secrets Manager and returns it as a dict."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)


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

    # Fall back to Secrets Manager (Lambda / prod)
    secret = get_secret(os.environ["DB_SECRET_NAME"])
    return psycopg2.connect(
        host=secret["host"],
        port=secret.get("port", 5432),
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],

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
@st.cache_data(ttl=1200, show_spinner="Fetching market data...")
def fetch_market_data(_conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    history_q = """
        SELECT
            s.ticker, s.stock_name,
            h.bar_date, h.open, h.high, h.low, h.close,
            h.volume, h.trade_count, h.vwap
        FROM alpaca_history h
        JOIN stock s ON s.stock_id = h.stock_id
        ORDER BY s.ticker, h.bar_date DESC
    """
    live_q = """
        SELECT
            s.ticker, s.stock_name,
            l.latest_time, l.open, l.high, l.low, l.close,
            l.volume, l.trade_count, l.vwap
        FROM alpaca_live l
        JOIN stock s ON s.stock_id = l.stock_id
        ORDER BY l.latest_time DESC
    """
    return pd.read_sql(history_q, _conn), pd.read_sql(live_q, _conn)


@st.cache_data(ttl=1200, show_spinner="Fetching news articles...")
def fetch_news(_conn) -> pd.DataFrame:
    q = """
        SELECT
            a.story_id, a.title, a.url, a.summary, a.published_date, a.source,
            ss.sentiment_score, ss.relevance_score, ss.analysis,
            s.ticker
        FROM rss_article a
        JOIN story_stock ss ON ss.story_id = a.story_id
        JOIN stock s        ON s.stock_id  = ss.stock_id
        ORDER BY a.published_date DESC
    """
    return pd.read_sql(q, _conn)


@st.cache_data(ttl=1200, show_spinner="Fetching Reddit posts...")
def fetch_reddit(_conn) -> pd.DataFrame:
    q = """
        SELECT
            p.post_id, p.title, p.contents, p.flair, p.score,
            p.upvote_ratio, p.num_comments, p.author, p.created_at,
            p.permalink, sub.subreddit_name
        FROM reddit_post p
        JOIN subreddit sub ON sub.subreddit_id = p.subreddit_id
        ORDER BY p.created_at DESC
    """
    return pd.read_sql(q, _conn)


# ---------------------------------------------------------------------------
# App title
# ---------------------------------------------------------------------------
st.title("📈 Tech Stock Research")

if conn is None:
    st.error("Could not establish a database connection. Check logs for details.")
    st.stop()


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_market, tab_news, tab_reddit = st.tabs(["Market Data", "News", "Reddit"])


# ── Tab 1: Market Data ───────────────────────────────────────────────────────
with tab_market:
    st.subheader("Market Data")

    try:
        df_history, df_live = fetch_market_data(conn)
    except Exception as e:
        st.error(f"Failed to load market data: {e}")
        st.stop()

    tickers = sorted(df_history["ticker"].unique().tolist())
    selected = st.selectbox("Select ticker", tickers)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Price History")
        df_sel = df_history[df_history["ticker"] == selected].copy()
        st.dataframe(df_sel, use_container_width=True, hide_index=True)

    with col2:
        st.markdown("#### Live Snapshot")
        df_live_sel = df_live[df_live["ticker"] == selected]
        st.dataframe(df_live_sel, use_container_width=True, hide_index=True)

    st.markdown("#### Closing Price Over Time")
    if not df_sel.empty:
        chart = (
            alt.Chart(df_sel)
            .mark_line(point=True)
            .encode(
                x=alt.X("bar_date:T", title="Date"),
                y=alt.Y("close:Q", title="Close Price (USD)",
                        scale=alt.Scale(zero=False)),
                tooltip=["bar_date:T", "open:Q", "high:Q",
                         "low:Q", "close:Q", "volume:Q"],
            )
            .properties(height=350)
            .interactive()
        )
        st.altair_chart(chart, use_container_width=True)


# ── Tab 2: News ──────────────────────────────────────────────────────────────
with tab_news:
    st.subheader("News Articles")

    try:
        df_news = fetch_news(conn)
    except Exception as e:
        st.error(f"Failed to load news: {e}")
        st.stop()

    col1, col2 = st.columns(2)
    with col1:
        ticker_filter = st.multiselect(
            "Filter by ticker", sorted(df_news["ticker"].unique()))
    with col2:
        source_filter = st.multiselect(
            "Filter by source", sorted(df_news["source"].unique()))

    filtered = df_news.copy()
    if ticker_filter:
        filtered = filtered[filtered["ticker"].isin(ticker_filter)]
    if source_filter:
        filtered = filtered[filtered["source"].isin(source_filter)]

    st.dataframe(
        filtered[["published_date", "ticker", "source", "title",
                  "sentiment_score", "relevance_score", "summary"]],
        use_container_width=True,
        hide_index=True,
    )


# ── Tab 3: Reddit ────────────────────────────────────────────────────────────
with tab_reddit:
    st.subheader("Reddit Posts")

    try:
        df_reddit = fetch_reddit(conn)
    except Exception as e:
        st.error(f"Failed to load Reddit data: {e}")
        st.stop()

    subreddit_filter = st.multiselect(
        "Filter by subreddit", sorted(df_reddit["subreddit_name"].unique()))

    filtered_r = df_reddit.copy()
    if subreddit_filter:
        filtered_r = filtered_r[filtered_r["subreddit_name"].isin(
            subreddit_filter)]

    st.dataframe(
        filtered_r[["created_at", "subreddit_name", "title", "score",
                    "upvote_ratio", "num_comments", "author", "flair"]],
        use_container_width=True,
        hide_index=True,
    )
