"""
app.py

Streamlit dashboard for the Tech Stock Research Tool.
Connects to PostgreSQL RDS and displays data across three tabs.

Run:
    streamlit run app.py
"""

from .queries import HISTORY_QUERY, LIVE_QUERY, SENTIMENT_QUERY, NEWS_QUERY, REDDIT_QUERY, RETURN_VOLATILITY_QUERY
from .charts import build_stacked_bar_chart, build_price_line_chart, build_sentiment_lollipop_chart
from .helpers import (
    TIME_OPTIONS,
    apply_time_filter,
    add_daily_returns,
    get_period_short_label,
    build_return_volatility_table,
    render_return_volatility_section,
)
import psycopg2
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import os
import json
import boto3

load_dotenv()


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

    # Fall back to Secrets Manager (Lambda / prod)
    secret = get_secret(os.environ["SECRETS_REPO_NAME"])
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
    except Exception as e:
        raise RuntimeError("Database connection failed")

conn = st.session_state.conn


# ---------------------------------------------------------------------------
# Cached queries (20-minute TTL)
# ---------------------------------------------------------------------------
# Import queries from queries.py


@st.cache_data(ttl=1200, show_spinner="Fetching market data...")
def fetch_market_data(_conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    return pd.read_sql(HISTORY_QUERY, _conn), pd.read_sql(LIVE_QUERY, _conn)


@st.cache_data(ttl=1200, show_spinner="Fetching sentiment data...")
def fetch_sentiment(_conn) -> pd.DataFrame:
    return pd.read_sql(SENTIMENT_QUERY, _conn)


@st.cache_data(ttl=1200, show_spinner="Fetching news articles...")
def fetch_news(_conn) -> pd.DataFrame:
    return pd.read_sql(NEWS_QUERY, _conn)


@st.cache_data(ttl=1200, show_spinner="Fetching Reddit posts...")
def fetch_reddit(_conn) -> pd.DataFrame:
    return pd.read_sql(REDDIT_QUERY, _conn)


@st.cache_data(ttl=1200, show_spinner="Fetching return/volatility data...")
def fetch_return_volatility_data(_conn) -> pd.DataFrame:
    """Load and sort historical price data for return/volatility computation."""
    dataframe = pd.read_sql(RETURN_VOLATILITY_QUERY, _conn)
    dataframe["bar_date"] = pd.to_datetime(dataframe["bar_date"])
    return dataframe.sort_values(["ticker", "bar_date"]).reset_index(drop=True)


def dashboard():
    if conn is None:
        st.error("No database connection. Check the sidebar for connection details.")
        return

    # ── Time Range Filter ────────────────────────────────────────────────────────
    time_label = st.radio(
        "Time Range",
        list(TIME_OPTIONS.keys()),
        horizontal=True,
        key="trends_time_range",
    )
    time_days = TIME_OPTIONS[time_label]

    st.divider()

    # ── PRIMARY: Return vs Volatility ─────────────────────────────────────────────────────
    # This is the most important chart — appears first
    try:
        rv_df_raw = fetch_return_volatility_data(conn)
    except Exception as e:
        st.error(f"Failed to load return/volatility data: {e}")
        return

    rv_df = add_daily_returns(rv_df_raw)
    period_short_label = get_period_short_label(time_label)
    metrics_df = build_return_volatility_table(
        rv_df, time_days, period_short_label)

    if metrics_df.empty:
        st.info(
            "Not enough data to calculate return/volatility metrics for this period.")
    else:
        rv_title, rv_info = st.columns([6, 1])
        with rv_title:
            st.subheader("Return vs Volatility Landscape")
        with rv_info:
            with st.popover("ℹ️"):
                st.markdown(
                    "**Return vs Volatility Landscape**\n\n"
                    "A high-level view of every tracked ticker's risk-return "
                    "profile for the selected period.\n\n"
                    "- **KPIs** — best/worst return and lowest/highest "
                    "volatility tickers.\n"
                    "- **Scatter chart** — each dot is a ticker; x-axis is "
                    "annualised volatility, y-axis is total return.\n"
                    "- Use the **ticker view** radio to toggle between all "
                    "tickers and the top/bottom movers."
                )
        st.caption("See how each stock balances growth with volatility. Size shows total return; position shows risk. Click a ticker to highlight it.")
        render_return_volatility_section(metrics_df, period_short_label)

    st.divider()

    # ── Market Data ──────────────────────────────────────────────────────────────

    try:
        df_history_raw, df_live = fetch_market_data(conn)
        df_sentiment_raw = fetch_sentiment(conn)
    except Exception as e:
        st.error(f"Failed to load market data: {e}")
        st.stop()

    # Apply time filter to history
    df_history = apply_time_filter(
        df_history_raw.copy(), "bar_date", time_days)

    # Compute relative volume per ticker:
    # avg volume in selected period / avg volume across all time
    avg_all = df_history_raw.groupby(
        "ticker")["volume"].mean().rename("avg_vol_all")
    avg_per = df_history.groupby("ticker").agg(
        avg_volume=("volume", "mean"),
        avg_trade_count=("trade_count", "mean"),
    ).join(avg_all)
    avg_per["relative_volume"] = avg_per["avg_volume"] / avg_per["avg_vol_all"]
    avg_per = avg_per.reset_index()

    col_left, col_right = st.columns(2)

    # ── Chart 2: Relative volume vs trade count ────────────────
    with col_left:
        chart2_title, chart2_info = st.columns([6, 1])
        with chart2_title:
            st.markdown("#### Relative Volume vs Trade Count")
        with chart2_info:
            with st.popover("ℹ️"):
                st.markdown(
                    "**Relative Volume vs Trade Count**\n\n"
                    "Compares two normalised metrics side-by-side for each ticker "
                    "over the selected time period:\n\n"
                    "- **Relative Volume** — average daily volume in the period "
                    "divided by the all-time average. Values above 1 indicate "
                    "above-average activity.\n"
                    "- **Trade Count** — average number of individual trades per day.\n\n"
                    "Both are scaled to 0–1 so they can be stacked on the same axis."
                )

        # Normalise both metrics to 0–1 for stacking on same axis
        all_tickers_bar = sorted(avg_per["ticker"].unique().tolist())
        selected_tickers_bar = st.multiselect(
            "Select tickers to compare",
            options=all_tickers_bar,
            default=all_tickers_bar[:14] if len(
                all_tickers_bar) >= 3 else all_tickers_bar,
            key="bar_ticker_select",
        )
        avg_per_filtered = avg_per[avg_per["ticker"].isin(
            selected_tickers_bar)]

        stacked_chart = build_stacked_bar_chart(avg_per, avg_per_filtered)
        st.altair_chart(stacked_chart, use_container_width=True)

    # ── Chart 4: Close price over time — multi-ticker ─────────────────────────
    with col_right:
        chart4_title, chart4_info = st.columns([6, 1])
        with chart4_title:
            st.markdown("#### Close Price Over Time")
        with chart4_info:
            with st.popover("ℹ️"):
                st.markdown(
                    "**Close Price Over Time**\n\n"
                    "Shows the daily closing price for each selected ticker across "
                    "the chosen time range.\n\n"
                    "- Use the **multiselect** above the chart to add or remove tickers.\n"
                    "- **Click a ticker in the legend** to highlight it and fade the rest.\n"
                    "- The chart is interactive — scroll to zoom, click-drag to pan."
                )

        all_tickers = sorted(df_history["ticker"].unique().tolist())
        selected_tickers = st.multiselect(
            "Select tickers to compare",
            options=all_tickers,
            default=['AAPL', 'AMZN', 'MSFT'] if len(all_tickers) >= 3 and all(
                ticker in all_tickers for ticker in ['AAPL', 'AMZN', 'MSFT']) else all_tickers,
        )

        df_line = df_history[df_history["ticker"].isin(selected_tickers)]

        if df_line.empty:
            st.info("Select at least one ticker above.")
        else:
            line_chart = build_price_line_chart(df_line)
            st.altair_chart(line_chart, use_container_width=True)

    # ── Chart 5: Combined sentiment lollipop (avg across sources) ─────────────
    chart5_title, chart5_info = st.columns([6, 1])
    with chart5_title:
        st.markdown("#### Avg Sentiment by Ticker")
    with chart5_info:
        with st.popover("ℹ️"):
            st.markdown(
                "**Combined Avg Sentiment by Ticker**\n\n"
                "Displays the average sentiment score per ticker, aggregated "
                "across all sources (news articles and Reddit) within the selected "
                "time period.\n\n"
                "- Scores range from **-1** (very negative) to **+1** (very positive).\n"
                "- 🟢 **Green** stems indicate a net positive sentiment.\n"
                "- 🔴 **Red** stems indicate a net negative sentiment.\n"
                "- Tickers are sorted from most positive to most negative."
            )

    df_combined = (
        apply_time_filter(df_sentiment_raw.copy(), "published_at", time_days)
        .groupby("ticker")["sentiment_score"]
        .mean()
        .reset_index()
    )
    df_combined["direction"] = df_combined["sentiment_score"].apply(
        lambda s: "Positive" if s >= 0 else "Negative"
    )

    combined_lollipop = build_sentiment_lollipop_chart(df_combined)
    st.altair_chart(combined_lollipop, use_container_width=True)
    st.divider()
