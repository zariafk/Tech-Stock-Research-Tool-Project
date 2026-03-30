"""
Streamlit dashboard for return, volatility, and activity trends in tech equities.
"""

import os
import math
import pandas as pd
import psycopg2
import streamlit as st
import altair as alt
from dotenv import load_dotenv

st.set_page_config(page_title="Alpaca API Dashboard", layout="wide")
load_dotenv()

PERIOD_OPTIONS = ["1 Month", "3 Months", "6 Months", "1 Year", "Since Start"]
CHART_SCOPE_OPTIONS = ["All Tickers", "Top 10", "Bottom 10"]
RANK_OPTIONS = ["Top 5", "Bottom 5"]

NUM_SCOPE_TICKERS = 10
NUM_RANK_ROWS = 5
NUM_TREND_CHUNKS = 10


def get_db_connection():
    """Create and return a connection to the RDS database."""
    return psycopg2.connect(
        host=os.environ["host"],
        port=os.environ["port"],
        dbname=os.environ["dbname"],
        user=os.environ["username"],
        password=os.environ["password"]
    )


@st.cache_data(ttl=600)
def load_historical_data():
    """Load historical price and trade count data from RDS."""
    query = """
        SELECT
            h.stock_id,
            s.ticker,
            h.bar_date,
            h.close,
            h.trade_count
        FROM alpaca_history h
        JOIN stock s
            ON h.stock_id = s.stock_id
        WHERE h.close IS NOT NULL
        ORDER BY s.ticker, h.bar_date;
    """

    connection = get_db_connection()

    try:
        dataframe = pd.read_sql(query, connection)
    finally:
        connection.close()

    dataframe["bar_date"] = pd.to_datetime(dataframe["bar_date"])
    dataframe = dataframe.sort_values(
        ["ticker", "bar_date"]).reset_index(drop=True)

    return dataframe


def add_daily_returns(dataframe):
    """Add daily returns for each ticker."""
    result_frames = []

    unique_tickers = dataframe["ticker"].unique()

    for ticker in unique_tickers:
        ticker_df = dataframe[dataframe["ticker"] == ticker].copy()
        ticker_df = ticker_df.sort_values("bar_date")
        ticker_df["daily_return"] = ticker_df["close"].pct_change()
        result_frames.append(ticker_df)

    return pd.concat(result_frames, ignore_index=True)


def get_period_days(period_label):
    """Map lookback label to calendar days."""
    if period_label == "1 Month":
        return 30
    if period_label == "3 Months":
        return 90
    if period_label == "6 Months":
        return 180
    if period_label == "1 Year":
        return 365
    return None


def get_period_short_label(period_label):
    """Return short label for chart titles and tables."""
    if period_label == "1 Month":
        return "1M"
    if period_label == "3 Months":
        return "3M"
    if period_label == "6 Months":
        return "6M"
    if period_label == "1 Year":
        return "1Y"
    return "Since Start"


def filter_data_for_period(dataframe, period_label):
    """Filter the dataset to the selected period."""
    if period_label == "Since Start":
        return dataframe.copy()

    latest_date = dataframe["bar_date"].max()
    days = get_period_days(period_label)
    cutoff_date = latest_date - pd.Timedelta(days=days)

    return dataframe[dataframe["bar_date"] >= cutoff_date].copy()


def calculate_period_metrics(ticker_df, period_short_label):
    """Calculate period return and annualised volatility for one ticker."""
    ticker_df = ticker_df.sort_values("bar_date").copy()
    ticker_df = ticker_df.dropna(subset=["close"])

    if len(ticker_df) < 2:
        return None

    start_close = ticker_df.iloc[0]["close"]
    end_close = ticker_df.iloc[-1]["close"]

    if start_close == 0:
        return None

    period_return = (end_close / start_close) - 1

    returns_series = ticker_df["daily_return"].dropna()

    if len(returns_series) < 2:
        return None

    daily_volatility = returns_series.std()
    annualised_volatility = daily_volatility * math.sqrt(252)

    return {
        "ticker": ticker_df.iloc[0]["ticker"],
        "period": period_short_label,
        "return_pct": period_return * 100,
        "volatility_pct": annualised_volatility * 100
    }


def build_return_volatility_table(dataframe, period_label):
    """Compute return and volatility for every ticker in the selected period."""
    filtered_df = filter_data_for_period(dataframe, period_label)
    period_short_label = get_period_short_label(period_label)

    results = []
    unique_tickers = filtered_df["ticker"].unique()

    for ticker in unique_tickers:
        ticker_df = filtered_df[filtered_df["ticker"] == ticker].copy()
        metric_row = calculate_period_metrics(ticker_df, period_short_label)

        if metric_row is not None:
            results.append(metric_row)

    results_df = pd.DataFrame(results)

    if not results_df.empty:
        results_df = results_df.sort_values(
            "return_pct", ascending=False).reset_index(drop=True)

    return results_df


def filter_metrics_by_scope(metrics_df, scope_label):
    """Filter chart data to all tickers, top 10, or bottom 10 by return."""
    if scope_label == "All Tickers":
        return metrics_df.copy()

    if scope_label == "Top 10":
        return metrics_df.sort_values("return_pct", ascending=False).head(NUM_SCOPE_TICKERS).copy()

    return metrics_df.sort_values("return_pct", ascending=True).head(NUM_SCOPE_TICKERS).copy()


def build_rank_table(metrics_df, view_option):
    """Build the top 5 or bottom 5 ranking table."""
    if view_option == "Top 5":
        rank_df = metrics_df.sort_values(
            "return_pct", ascending=False).head(NUM_RANK_ROWS).copy()
    else:
        rank_df = metrics_df.sort_values(
            "return_pct", ascending=True).head(NUM_RANK_ROWS).copy()

    rank_df["return_pct"] = rank_df["return_pct"].round(2)
    rank_df["volatility_pct"] = rank_df["volatility_pct"].round(2)

    return rank_df.rename(
        columns={
            "ticker": "Ticker",
            "period": "Period",
            "return_pct": "Return %",
            "volatility_pct": "Volatility %"
        }
    )


def format_chunk_label(start_date, end_date, period_label):
    """Create a readable label for each time chunk."""
    if period_label in ("1 Month", "3 Months"):
        return f"{start_date.strftime('%d %b')} - {end_date.strftime('%d %b')}"
    return start_date.strftime("%b '%y")


def compute_average_trade_count(period_df, tickers):
    """Compute the average trade count per ticker across the selected period."""
    averages = {}

    for ticker in tickers:
        ticker_df = period_df[period_df["ticker"] == ticker].copy()
        valid_counts = ticker_df["trade_count"].dropna()

        if len(valid_counts) == 0:
            averages[ticker] = 0
        else:
            averages[ticker] = valid_counts.mean()

    return averages


def build_trend_data(dataframe, period_label, filtered_metrics_df):
    """
    Build chunked trend data for return % and trade count ratio.

    Return % is computed across each chunk.
    Trade count ratio compares chunk average trade count with the ticker's
    average trade count across the full selected period.
    """
    period_df = filter_data_for_period(dataframe, period_label)

    if period_df.empty or filtered_metrics_df.empty:
        return pd.DataFrame()

    selected_tickers = filtered_metrics_df["ticker"].tolist()
    period_df = period_df[period_df["ticker"].isin(selected_tickers)].copy()

    if period_df.empty:
        return pd.DataFrame()

    sorted_dates = sorted(period_df["bar_date"].drop_duplicates().tolist())
    num_dates = len(sorted_dates)

    if num_dates < 2:
        return pd.DataFrame()

    num_chunks = min(NUM_TREND_CHUNKS, num_dates)
    chunk_size = num_dates / num_chunks
    avg_trade_counts = compute_average_trade_count(period_df, selected_tickers)

    rows = []

    for chunk_index in range(num_chunks):
        start_pos = int(chunk_index * chunk_size)
        end_pos = int((chunk_index + 1) * chunk_size)

        if chunk_index == num_chunks - 1:
            end_pos = num_dates

        chunk_dates = sorted_dates[start_pos:end_pos]

        if len(chunk_dates) == 0:
            continue

        chunk_start_date = pd.Timestamp(chunk_dates[0]).date()
        chunk_end_date = pd.Timestamp(chunk_dates[-1]).date()
        chunk_label = format_chunk_label(
            chunk_start_date, chunk_end_date, period_label)

        chunk_df = period_df[period_df["bar_date"].isin(chunk_dates)].copy()

        for ticker in selected_tickers:
            ticker_chunk = chunk_df[chunk_df["ticker"]
                                    == ticker].sort_values("bar_date").copy()

            if len(ticker_chunk) < 2:
                continue

            start_close = ticker_chunk.iloc[0]["close"]
            end_close = ticker_chunk.iloc[-1]["close"]

            if pd.isna(start_close) or start_close == 0:
                continue

            return_pct = ((end_close / start_close) - 1) * 100

            chunk_avg_trade_count = ticker_chunk["trade_count"].dropna().mean()
            ticker_avg_trade_count = avg_trade_counts.get(ticker, 0)

            if pd.isna(ticker_avg_trade_count) or ticker_avg_trade_count == 0:
                trade_count_ratio = None
            else:
                trade_count_ratio = chunk_avg_trade_count / ticker_avg_trade_count

            rows.append(
                {
                    "ticker": ticker,
                    "chunk_index": chunk_index,
                    "chunk_label": chunk_label,
                    "chunk_start": chunk_start_date,
                    "chunk_end": chunk_end_date,
                    "return_pct": return_pct,
                    "trade_count_ratio": trade_count_ratio
                }
            )

    return pd.DataFrame(rows)


def create_return_volatility_chart(metrics_df, period_label):
    """Create the Altair return vs volatility scatter chart."""
    plot_df = metrics_df.copy()
    plot_df["abs_return"] = plot_df["return_pct"].abs()

    period_short_label = get_period_short_label(period_label)

    selected_ticker = alt.selection_point(
        fields=["ticker"], on="click", clear="dblclick")
    zero_line_df = pd.DataFrame({"y_value": [0]})

    zero_line = alt.Chart(zero_line_df).mark_rule(
        strokeDash=[6, 6],
        color="#9CA3AF"
    ).encode(y="y_value:Q")

    points = alt.Chart(plot_df).mark_circle(stroke="white", strokeWidth=1).encode(
        x=alt.X("volatility_pct:Q",
                title=f"{period_short_label} Volatility (%)"),
        y=alt.Y("return_pct:Q", title=f"{period_short_label} Return (%)"),
        color=alt.Color(
            "return_pct:Q",
            title="Return %",
            scale=alt.Scale(scheme="redyellowgreen", domainMid=0)
        ),
        size=alt.Size(
            "abs_return:Q",
            title="Bubble size = absolute return %",
            scale=alt.Scale(range=[80, 500]),
            legend=alt.Legend(orient="right")
        ),
        opacity=alt.condition(
            selected_ticker, alt.value(1.0), alt.value(0.85)),
        tooltip=[
            alt.Tooltip("ticker:N", title="Ticker"),
            alt.Tooltip("period:N", title="Period"),
            alt.Tooltip("return_pct:Q", title="Return %", format=".2f"),
            alt.Tooltip("volatility_pct:Q", title="Volatility %", format=".2f")
        ]
    ).add_params(selected_ticker)

    labels = alt.Chart(plot_df).mark_text(
        align="left",
        baseline="middle",
        dx=8,
        dy=-6,
        fontSize=11,
        color="#E5E7EB"
    ).encode(
        x="volatility_pct:Q",
        y="return_pct:Q",
        text="ticker:N",
        opacity=alt.condition(selected_ticker, alt.value(1.0), alt.value(0.95))
    )

    chart = (zero_line + points + labels).properties(
        title=f"Return vs Volatility ({period_short_label})",
        height=560
    ).configure_view(
        strokeOpacity=0
    ).configure_axis(
        labelColor="#E5E7EB",
        titleColor="#E5E7EB",
        gridColor="#253041"
    ).configure_legend(
        labelColor="#E5E7EB",
        titleColor="#E5E7EB"
    ).configure_title(
        color="#E5E7EB",
        fontSize=20
    )

    return chart


def create_return_activity_trend_chart(trend_df, period_label):
    """Create a two-panel trend chart for return % and trade count ratio."""
    if trend_df.empty:
        return alt.Chart(pd.DataFrame()).mark_line()

    period_short_label = get_period_short_label(period_label)
    chunk_order = (
        trend_df[["chunk_index", "chunk_label"]]
        .drop_duplicates()
        .sort_values("chunk_index")["chunk_label"]
        .tolist()
    )

    base = alt.Chart(trend_df).encode(
        x=alt.X(
            "chunk_label:N",
            sort=chunk_order,
            title=f"Time Buckets ({period_short_label})",
            axis=alt.Axis(labelAngle=-30)
        ),
        color=alt.Color("ticker:N", title="Ticker"),
        tooltip=[
            alt.Tooltip("ticker:N", title="Ticker"),
            alt.Tooltip("chunk_label:N", title="Time Bucket"),
            alt.Tooltip("chunk_start:N", title="From"),
            alt.Tooltip("chunk_end:N", title="To"),
            alt.Tooltip("return_pct:Q", title="Return %", format=".2f"),
            alt.Tooltip("trade_count_ratio:Q",
                        title="Trade Count Ratio", format=".2f")
        ]
    )

    return_rule = alt.Chart(pd.DataFrame({"y_value": [0]})).mark_rule(
        strokeDash=[6, 6],
        color="#9CA3AF"
    ).encode(y="y_value:Q")

    trade_rule = alt.Chart(pd.DataFrame({"y_value": [1]})).mark_rule(
        strokeDash=[6, 6],
        color="#9CA3AF"
    ).encode(y="y_value:Q")

    return_chart = (return_rule + base.mark_line(point=True).encode(
        y=alt.Y("return_pct:Q", title="Chunk Return (%)")
    )).properties(
        title=f"Return Trend by Time Bucket ({period_short_label})",
        height=260
    )

    trade_chart = (trade_rule + base.mark_line(point=True).encode(
        y=alt.Y("trade_count_ratio:Q", title="Trade Count Ratio")
    )).properties(
        title="Trade Count Ratio Trend",
        height=260
    )

    chart = alt.vconcat(return_chart, trade_chart).resolve_scale(color="shared").configure_view(
        strokeOpacity=0
    ).configure_axis(
        labelColor="#E5E7EB",
        titleColor="#E5E7EB",
        gridColor="#253041"
    ).configure_legend(
        labelColor="#E5E7EB",
        titleColor="#E5E7EB"
    ).configure_title(
        color="#E5E7EB",
        fontSize=18
    )

    return chart


def render_return_volatility_popover():
    """Render the info popover for the scatter chart."""
    with st.popover("About this chart"):
        st.markdown(
            """
            **What it shows**
            - Each dot is a ticker.
            - The x-axis is annualised volatility for the selected lookback period.
            - The y-axis is total return over the selected lookback period.

            **Definitions**
            - **Return %** = `((end close / start close) - 1) × 100`
            - **Volatility %** = standard deviation of daily returns, annualised using `√252`
            - **Bubble size** = absolute return %, so bigger dots are stronger movers up or down

            **How to read it**
            - Higher up = stronger return
            - Further right = more volatile
            - Bigger bubble = larger absolute move
            """
        )


def render_trend_popover():
    """Render the info popover for the trend chart."""
    with st.popover("About this chart"):
        st.markdown(
            """
            **What it shows**
            - The selected period is split into time buckets.
            - The top panel shows return trend across those buckets.
            - The bottom panel shows trade count ratio across the same buckets.

            **Definitions**
            - **Chunk Return %** = return from the first close to the last close inside that time bucket
            - **Trade Count** = number of trades recorded in the daily bar data
            - **Trade Count Ratio** = `chunk average trade count / ticker average trade count for the full selected period`

            **How to read it**
            - A trade count ratio above `1.0` means activity is above that ticker's own normal level
            - A trade count ratio below `1.0` means activity is below normal
            """
        )


def main():
    st.title("Alpaca API Dashboard")
    st.write("Compare price performance, volatility, and activity trends across different lookback periods.")

    try:
        historical_df = load_historical_data()
    except Exception as error:
        st.error(f"Error loading data from RDS: {error}")
        return

    if historical_df.empty:
        st.warning("No historical data found.")
        return

    historical_df = add_daily_returns(historical_df)

    st.sidebar.header("Filters")

    selected_period = st.sidebar.radio(
        "Lookback period",
        PERIOD_OPTIONS,
        index=0
    )

    selected_scope = st.sidebar.radio(
        "Chart ticker scope",
        CHART_SCOPE_OPTIONS,
        index=0
    )

    metrics_df = build_return_volatility_table(historical_df, selected_period)

    if metrics_df.empty:
        st.warning("Not enough data to calculate metrics for this period.")
        return

    chart_metrics_df = filter_metrics_by_scope(metrics_df, selected_scope)
    trend_df = build_trend_data(
        historical_df, selected_period, chart_metrics_df)

    best_return_row = metrics_df.sort_values(
        "return_pct", ascending=False).iloc[0]
    worst_return_row = metrics_df.sort_values(
        "return_pct", ascending=True).iloc[0]
    lowest_vol_row = metrics_df.sort_values(
        "volatility_pct", ascending=True).iloc[0]
    highest_vol_row = metrics_df.sort_values(
        "volatility_pct", ascending=False).iloc[0]

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    with kpi_col1:
        st.metric(
            "Best Return", best_return_row["ticker"], f'{best_return_row["return_pct"]:.2f}%')

    with kpi_col2:
        st.metric(
            "Worst Return", worst_return_row["ticker"], f'{worst_return_row["return_pct"]:.2f}%')

    with kpi_col3:
        st.metric("Lowest Volatility",
                  lowest_vol_row["ticker"], f'{lowest_vol_row["volatility_pct"]:.2f}%')

    with kpi_col4:
        st.metric("Highest Volatility",
                  highest_vol_row["ticker"], f'{highest_vol_row["volatility_pct"]:.2f}%')

    st.caption(
        f"Charts are currently showing: **{selected_scope}** for the selected **{selected_period}** period."
    )

    chart_col1, chart_col2 = st.columns([1.2, 1])

    with chart_col1:
        header_col1, header_col2 = st.columns([5, 1])
        with header_col1:
            st.subheader("Return vs Volatility")
        with header_col2:
            render_return_volatility_popover()

        st.caption(
            "Hover to see exact values. Click a point to highlight that ticker.")
        return_vol_chart = create_return_volatility_chart(
            chart_metrics_df, selected_period)
        st.altair_chart(return_vol_chart, use_container_width=True)

    with chart_col2:
        header_col1, header_col2 = st.columns([5, 1])
        with header_col1:
            st.subheader("Return and Activity Trend")
        with header_col2:
            render_trend_popover()

        if trend_df.empty:
            st.warning(
                "Not enough data to build the trend chart for the selected filter.")
        else:
            trend_chart = create_return_activity_trend_chart(
                trend_df, selected_period)
            st.altair_chart(trend_chart, use_container_width=True)

    st.divider()

    table_header_col1, table_header_col2 = st.columns([5, 2])

    with table_header_col1:
        st.subheader("Return Ranking")

    with table_header_col2:
        rank_view = st.radio(
            "Show",
            RANK_OPTIONS,
            horizontal=True,
            key="rank_toggle"
        )

    rank_df = build_rank_table(metrics_df, rank_view)
    st.dataframe(rank_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
