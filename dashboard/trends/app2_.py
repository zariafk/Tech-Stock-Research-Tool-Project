"""
app.py

Streamlit dashboard for the Tech Stock Research Tool.
Connects to PostgreSQL RDS and displays data across three tabs.

Run:
    streamlit run app.py
"""

from queries import HISTORY_QUERY, LIVE_QUERY, SENTIMENT_QUERY, NEWS_QUERY, REDDIT_QUERY
import psycopg2
import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, timedelta
from dotenv import load_dotenv
import os

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


# ---------------------------------------------------------------------------
# Sidebar — Time filter
# ---------------------------------------------------------------------------
st.sidebar.header("Time Range")

TIME_OPTIONS = {
    "1 Month":    30,
    "3 Months":   90,
    "6 Months":   180,
    "1 Year":     365,
    "From Start": None,
}

time_label = st.sidebar.radio("Select period", list(TIME_OPTIONS.keys()))
time_days = TIME_OPTIONS[time_label]


def apply_time_filter(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if time_days is None:
        return df
    cutoff = pd.Timestamp(date.today() - timedelta(days=time_days), tz="UTC")
    df[date_col] = pd.to_datetime(df[date_col], utc=True)
    filtered_date = df[df[date_col].isna() | (df[date_col] >= cutoff)]
    return filtered_date


def dashboard():

    # ── Tab 1: Market Data ───────────────────────────────────────────────────────

    try:
        df_history_raw, df_live = fetch_market_data(conn)
        df_sentiment_raw = fetch_sentiment(conn)
    except Exception as e:
        st.error(f"Failed to load market data: {e}")
        st.stop()

    # Apply time filter to history
    df_history = apply_time_filter(df_history_raw.copy(), "bar_date")

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

    # ── Chart 2: Relative volume vs trade count — stacked bar ────────────────
    with col_right:
        chart2_title, chart2_info = st.columns([10, 1])
        with chart2_title:
            st.markdown("#### Relative Volume vs Trade Count — Stacked")
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
        vol_max = avg_per_filtered["relative_volume"].max()
        tc_max = avg_per_filtered["avg_trade_count"].max()
        stacked = pd.concat([
            avg_per_filtered[["ticker", "relative_volume"]].rename(columns={"relative_volume": "value"}).assign(
                metric="Relative Volume", value_norm=avg_per_filtered["relative_volume"] / vol_max
            ),
            avg_per_filtered[["ticker", "avg_trade_count"]].rename(columns={"avg_trade_count": "value"}).assign(
                metric="Trade Count", value_norm=avg_per_filtered["avg_trade_count"] / tc_max
            ),
        ], ignore_index=True)

        stacked_chart = (
            alt.Chart(stacked)
            .mark_bar()
            .encode(
                x=alt.X("ticker:N", title="Ticker",
                        axis=alt.Axis(labelAngle=-45)),
                y=alt.Y("value_norm:Q",
                        title="Normalised Value (0–1)", stack="zero"),
                color=alt.Color("metric:N", legend=alt.Legend(title="Metric")),
                tooltip=["ticker:N", "metric:N", alt.Tooltip("value:Q", format=".2f"), alt.Tooltip(
                    "value_norm:Q", format=".2f", title="Normalised")],
            )
            .properties(height=260)
            .interactive()
        )
        st.altair_chart(stacked_chart, use_container_width=True)

    # ── Chart 4: Close price over time — multi-ticker ─────────────────────────
    with col_right:
        chart4_title, chart4_info = st.columns([10, 1])
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
            highlight = alt.selection_point(fields=["ticker"], bind="legend")

            line_chart = (
                alt.Chart(df_line)
                .mark_line()
                .encode(
                    x=alt.X("bar_date:T", title="Date"),
                    y=alt.Y("close:Q", title="Close Price (USD)",
                            scale=alt.Scale(zero=False)),
                    color=alt.Color(
                        "ticker:N", legend=alt.Legend(title="Ticker")),
                    opacity=alt.condition(
                        highlight, alt.value(1), alt.value(0.15)),
                    tooltip=["ticker:N", "bar_date:T", alt.Tooltip(
                        "close:Q", format=".2f"), alt.Tooltip("volume:Q", format=",")],
                )
                .add_params(highlight)
                .properties(height=320)
                .interactive()
            )
            st.altair_chart(line_chart, use_container_width=True)

    # ── Chart 5: Combined sentiment lollipop (avg across sources) ─────────────
    chart5_title, chart5_info = st.columns([10, 1])
    with chart5_title:
        st.markdown("#### Combined Avg Sentiment by Ticker")
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
        apply_time_filter(df_sentiment_raw.copy(), "published_at")
        .groupby("ticker")["sentiment_score"]
        .mean()
        .reset_index()
    )
    df_combined["direction"] = df_combined["sentiment_score"].apply(
        lambda s: "Positive" if s >= 0 else "Negative"
    )

    sentiment_colour = alt.Color(
        "direction:N",
        scale=alt.Scale(domain=["Positive", "Negative"],
                        range=["#2ecc71", "#e74c3c"]),
        legend=alt.Legend(title="Sentiment"),
    )
    ticker_sort = alt.EncodingSortField(
        field="sentiment_score", op="mean", order="descending")

    ticker_axis = alt.Axis(labelAngle=-45, labelOverlap=False)

    combined_rule = (
        alt.Chart(df_combined)
        .mark_rule(strokeWidth=2)
        .encode(
            x=alt.X("ticker:N", title="Ticker", sort=ticker_sort,
                    axis=ticker_axis),
            y=alt.Y("sentiment_score:Q", title="Avg Sentiment Score",
                    scale=alt.Scale(domain=[-1, 1])),
            y2=alt.datum(0),
            color=sentiment_colour,
        )
    )

    combined_point = (
        alt.Chart(df_combined)
        .mark_point(size=100, filled=True)
        .encode(
            x=alt.X("ticker:N", title="Ticker", sort=ticker_sort,
                    axis=ticker_axis),
            y=alt.Y("sentiment_score:Q", title="Avg Sentiment Score",
                    scale=alt.Scale(domain=[-1, 1])),
            color=sentiment_colour,
            tooltip=["ticker:N", "direction:N",
                     alt.Tooltip("sentiment_score:Q", format=".3f")],
        )
    )

    combined_lollipop = (
        combined_rule + combined_point).properties(height=260)
    st.altair_chart(combined_lollipop, use_container_width=True)
