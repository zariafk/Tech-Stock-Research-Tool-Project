"""
Streamlit dashboard for return and volatility analysis in tech equities.
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

PERIOD_OPTIONS = [
    "1 Month", "3 Months", "6 Months", "1 Year", "Since Start"]
LOCAL_SCOPE_OPTIONS = [
    "All Tickers", "Top 10", "Bottom 10"]
NUM_SCOPE_TICKERS: int = 10
PERIOD_DAYS_MAP = {
    "1 Month": 30,
    "3 Months": 90,
    "6 Months": 180,
    "1 Year": 365
}
PERIOD_SHORT_LABEL_MAP = {
    "1 Month": "1M",
    "3 Months": "3M",
    "6 Months": "6M",
    "1 Year": "1Y",
    "Since Start": "Since Start"
}


def get_db_connection():
    """Create and return a connection to the RDS database."""
    try:
        return psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", 5432),
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        )
    except KeyError as error:
        raise EnvironmentError(
            f"Missing required environment variable: {error}") from error
    except psycopg2.OperationalError as error:
        raise ConnectionError(
            f"Failed to connect to RDS database: {error}") from error


@st.cache_data(ttl=600)
def load_historical_data() -> pd.DataFrame:
    """Load historical price and trade count data from RDS."""
    query = """
    SELECT
        h.stock_id,
        s.ticker,
        h.bar_date,
        h.close,
        h.volume,
        h.trade_count
    FROM alpaca_history h
    JOIN stock s
        ON h.stock_id = s.stock_id
    WHERE h.close IS NOT NULL
    ORDER BY s.ticker, h.bar_date;
    """
    connection = None
    try:
        connection = get_db_connection()
        dataframe = pd.read_sql(query, connection)
    except (psycopg2.DatabaseError, psycopg2.InterfaceError) as error:
        raise ValueError(f"Query execution failed: {error}") from error
    finally:
        if connection is not None:
            connection.close()

    dataframe["bar_date"] = pd.to_datetime(dataframe["bar_date"])
    dataframe = dataframe.sort_values(
        ["ticker", "bar_date"]).reset_index(drop=True)

    return dataframe


def add_daily_returns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Add daily returns for each ticker."""
    result_frames = []
    unique_tickers = dataframe["ticker"].unique()

    for ticker in unique_tickers:
        ticker_df = dataframe[dataframe["ticker"] == ticker].copy()
        ticker_df = ticker_df.sort_values("bar_date")
        ticker_df["daily_return"] = ticker_df["close"].pct_change()
        result_frames.append(ticker_df)

    return pd.concat(result_frames, ignore_index=True)


def get_period_days(period_label: str):
    """Map lookback label to calendar days."""
    return PERIOD_DAYS_MAP.get(period_label)


def get_period_short_label(period_label: str) -> str:
    """Return a short period label for chart titles."""
    return PERIOD_SHORT_LABEL_MAP.get(period_label, "Unknown")


def filter_data_for_period(
        dataframe: pd.DataFrame, period_label: str) -> pd.DataFrame:
    """Filter the dataset to the selected period."""
    if period_label == "Since Start":
        return dataframe.copy()

    latest_date = dataframe["bar_date"].max()
    days = get_period_days(period_label)

    if days is None:
        return dataframe.copy()

    cutoff_date = latest_date - pd.Timedelta(days=days)
    return dataframe[dataframe["bar_date"] >= cutoff_date].copy()


def calculate_period_metrics(
        ticker_df: pd.DataFrame, period_short_label: str):
    """Calculate return and annualised volatility for one ticker."""
    ticker_df = ticker_df.sort_values("bar_date").copy()
    ticker_df = ticker_df.dropna(subset=["close"])

    if len(ticker_df) < 2:
        return None

    start_close = ticker_df.iloc[0]["close"]
    end_close = ticker_df.iloc[-1]["close"]

    if pd.isna(start_close) or start_close == 0:
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


def build_return_volatility_table(
        dataframe: pd.DataFrame, period_label: str) -> pd.DataFrame:
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


def get_scope_tickers(
        metrics_df: pd.DataFrame, scope_label: str) -> list:
    """Return ticker list for a local chart scope."""
    if metrics_df.empty:
        return []

    if scope_label == "All Tickers":
        return metrics_df["ticker"].tolist()

    if scope_label == "Top 10":
        scoped_df = metrics_df.sort_values(
            "return_pct", ascending=False).head(NUM_SCOPE_TICKERS)
        return scoped_df["ticker"].tolist()

    if scope_label == "Bottom 10":
        scoped_df = metrics_df.sort_values(
            "return_pct", ascending=True).head(NUM_SCOPE_TICKERS)
        return scoped_df["ticker"].tolist()

    return []


def filter_metrics_by_tickers(
        metrics_df: pd.DataFrame, tickers: list) -> pd.DataFrame:
    """Filter metrics table to a ticker list."""
    if len(tickers) == 0:
        return pd.DataFrame(columns=metrics_df.columns)

    filtered_df = metrics_df[metrics_df["ticker"].isin(tickers)].copy()
    return filtered_df.reset_index(drop=True)


def format_chunk_label(
        start_date, end_date, period_label: str) -> str:
    """Create a readable label for each time bucket."""
    if period_label in ("1 Month", "3 Months"):
        return f"{start_date.strftime('%d %b')} - {end_date.strftime('%d %b')}"
    return start_date.strftime("%b '%y")


def build_zero_line():
    """Build the zero reference line for the scatter plot."""
    zero_line_df = pd.DataFrame({"y_value": [0]})
    return alt.Chart(zero_line_df).mark_rule(
        strokeDash=[6, 6],
        color="#9CA3AF"
    ).encode(y="y_value:Q")


def build_scatter_points(
        plot_df: pd.DataFrame,
        period_short_label: str,
        selected_ticker):
    """Build the scatter plot points with encodings."""
    return alt.Chart(plot_df).mark_circle(
        stroke="white",
        strokeWidth=1
    ).encode(
        x=alt.X(
            "volatility_pct:Q",
            title=f"{period_short_label} Volatility (%)"
        ),
        y=alt.Y(
            "return_pct:Q",
            title=f"{period_short_label} Return (%)"
        ),
        color=alt.Color(
            "return_pct:Q",
            title="Return %",
            scale=alt.Scale(scheme="redyellowgreen", domainMid=0)
        ),

        opacity=alt.condition(
            selected_ticker,
            alt.value(1.0),
            alt.value(0.85)
        ),
        tooltip=[
            alt.Tooltip("ticker:N", title="Ticker"),
            alt.Tooltip("period:N", title="Period"),
            alt.Tooltip("return_pct:Q", title="Return %", format=".2f"),
            alt.Tooltip("volatility_pct:Q",
                        title="Volatility %", format=".2f"),
            alt.Tooltip("abs_return:Q",
                        title="Absolute Return %", format=".2f")
        ]
    ).add_params(selected_ticker)


def build_ticker_labels(
        plot_df: pd.DataFrame,
        selected_ticker):
    """Build the ticker labels for each point."""
    return alt.Chart(plot_df).mark_text(
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
        opacity=alt.condition(
            selected_ticker,
            alt.value(1.0),
            alt.value(0.95)
        )
    )


def apply_chart_styling(chart, period_short_label: str):
    """Apply consistent styling and configuration to the chart."""
    return chart.properties(
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


def create_return_volatility_chart(
        metrics_df: pd.DataFrame, period_label: str):
    """Create the Altair return vs volatility scatter chart."""
    if metrics_df.empty:
        return alt.Chart(pd.DataFrame()).mark_circle()

    plot_df = metrics_df.copy()
    plot_df["abs_return"] = plot_df["return_pct"].abs()
    period_short_label = get_period_short_label(period_label)

    selected_ticker = alt.selection_point(
        fields=["ticker"], on="click", clear="dblclick")

    zero_line = build_zero_line()
    points = build_scatter_points(plot_df, period_short_label, selected_ticker)
    labels = build_ticker_labels(plot_df, selected_ticker)

    combined_chart = zero_line + points + labels
    return apply_chart_styling(combined_chart, period_short_label)


def render_return_volatility_popover() -> None:
    """Render the info popover for the return vs volatility chart."""
    with st.popover("About this chart"):
        st.markdown(
            """
            **What it shows**
            - Each dot is a ticker.
            - The x-axis is annualised volatility over the selected lookback period.
            - The y-axis is total return over the selected lookback period.

            **Definitions**
            - **Return %** = `((end close / start close) - 1) × 100`
            - **Volatility %** = standard deviation of daily returns, annualised using `√252`
            - **Absolute Return %** = `|Return %|`, used for bubble size

            **How to read it**
            - Higher up = stronger return
            - Further right = more volatile
            - Bigger bubble = larger absolute move
            """
        )


def render_kpi_metrics(metrics_df: pd.DataFrame) -> None:
    """Render the KPI metrics row."""
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
            "Best Return",
            best_return_row["ticker"],
            f'{best_return_row["return_pct"]:.2f}%')

    with kpi_col2:
        st.metric(
            "Worst Return",
            worst_return_row["ticker"],
            f'{worst_return_row["return_pct"]:.2f}%')

    with kpi_col3:
        st.metric(
            "Lowest Volatility",
            lowest_vol_row["ticker"],
            f'{lowest_vol_row["volatility_pct"]:.2f}%')

    with kpi_col4:
        st.metric(
            "Highest Volatility",
            highest_vol_row["ticker"],
            f'{highest_vol_row["volatility_pct"]:.2f}%')


def render_return_volatility_section(
        metrics_df: pd.DataFrame, selected_period: str) -> None:
    """Render the return vs volatility chart section."""
    header_col1, header_col2 = st.columns([5, 1])

    with header_col1:
        st.subheader("Return vs Volatility")

    with header_col2:
        render_return_volatility_popover()

    return_vol_scope = st.radio(
        "Ticker view",
        LOCAL_SCOPE_OPTIONS,
        horizontal=True,
        key="return_vol_scope"
    )

    return_vol_tickers = get_scope_tickers(metrics_df, return_vol_scope)
    return_vol_metrics_df = filter_metrics_by_tickers(
        metrics_df, return_vol_tickers)

    st.caption(
        "Hover to see exact values. Click a point to highlight that ticker.")
    return_vol_chart = create_return_volatility_chart(
        return_vol_metrics_df, selected_period)
    st.altair_chart(return_vol_chart, use_container_width=True)

    min_ret = round(return_vol_metrics_df["return_pct"].abs().min(), 1)
    max_ret = round(return_vol_metrics_df["return_pct"].abs().max(), 1)
    st.caption(
        "Bubble size = absolute return %%  ·  smaller (~%s%%) = lower move  ·  larger (~%s%%) = stronger mover" % (
            min_ret, max_ret)
    )


def main() -> None:
    """Main Streamlit dashboard entry point."""
    st.title("Alpaca API Dashboard")
    st.write(
        "Compare price performance and volatility across different lookback periods.")

    try:
        historical_df = load_historical_data()
    except (EnvironmentError, ConnectionError) as error:
        st.error(f"Error loading data from RDS: {error}")
        return
    except ValueError as error:
        st.error(f"Error querying RDS: {error}")
        return

    if historical_df.empty:
        st.warning("No historical data found.")
        return

    historical_df = add_daily_returns(historical_df)

    st.sidebar.header("Filters")
    selected_period = st.sidebar.radio(
        "Lookback period", PERIOD_OPTIONS, index=0)

    metrics_df = build_return_volatility_table(historical_df, selected_period)

    if metrics_df.empty:
        st.warning("Not enough data to calculate metrics for this period.")
        return

    render_kpi_metrics(metrics_df)
    render_return_volatility_section(metrics_df, selected_period)


if __name__ == "__main__":
    main()
