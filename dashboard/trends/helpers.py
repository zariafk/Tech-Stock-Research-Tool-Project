"""
helpers.py

Constants, computation helpers, and render functions for the trends dashboard.
"""

import math
import streamlit as st
import pandas as pd
from .charts import create_return_volatility_chart


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TIME_OPTIONS: dict[str, int | None] = {
    "1 Month":    30,
    "3 Months":   90,
    "6 Months":   180,
    "1 Year":     365,
    "All History (From 2024)": None,
}

PERIOD_SHORT_LABEL_MAP: dict[str, str] = {
    "1 Month":    "1M",
    "3 Months":   "3M",
    "6 Months":   "6M",
    "1 Year":     "1Y",
    "All History (From 2024)": "All",
}

LOCAL_SCOPE_OPTIONS: list[str] = ["All Tickers", "Top 10", "Bottom 10"]
NUM_SCOPE_TICKERS: int = 10
ANNUALISED_TRADING_DAYS: int = 252
MIN_ROWS_FOR_METRICS: int = 2


# ---------------------------------------------------------------------------
# Time filter
# ---------------------------------------------------------------------------
def apply_time_filter(df: pd.DataFrame, date_col: str, time_days: int | None) -> pd.DataFrame:
    """Filter a DataFrame to rows within the last time_days days; return all if None."""
    from datetime import date, timedelta
    if time_days is None:
        return df
    cutoff = pd.Timestamp(date.today() - timedelta(days=time_days), tz="UTC")
    df[date_col] = pd.to_datetime(df[date_col], utc=True)
    return df[df[date_col].isna() | (df[date_col] >= cutoff)]


# ---------------------------------------------------------------------------
# Return vs Volatility — computation
# ---------------------------------------------------------------------------
def add_daily_returns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Add a daily_return column computed per ticker from close prices."""
    result_frames = []
    for ticker in dataframe["ticker"].unique():
        ticker_df = dataframe[dataframe["ticker"] ==
                              ticker].copy().sort_values("bar_date")
        ticker_df["daily_return"] = ticker_df["close"].pct_change()
        result_frames.append(ticker_df)
    return pd.concat(result_frames, ignore_index=True)


def get_period_short_label(period_label: str) -> str:
    """Map a full period label to its short display string."""
    return PERIOD_SHORT_LABEL_MAP.get(period_label, "All")


def filter_data_for_period(dataframe: pd.DataFrame, time_days: int | None) -> pd.DataFrame:
    """Slice the dataframe to the most recent time_days calendar days, or return all."""
    if time_days is None:
        return dataframe.copy()
    latest_date = dataframe["bar_date"].max()
    cutoff_date = latest_date - pd.Timedelta(days=time_days)
    return dataframe[dataframe["bar_date"] >= cutoff_date].copy()


def calculate_period_metrics(ticker_df: pd.DataFrame, period_short_label: str) -> dict | None:
    """Compute total return and annualised volatility for a single ticker slice."""
    ticker_df = ticker_df.sort_values("bar_date").dropna(subset=["close"])
    if len(ticker_df) < MIN_ROWS_FOR_METRICS:
        return None
    start_close = ticker_df.iloc[0]["close"]
    end_close = ticker_df.iloc[-1]["close"]
    if pd.isna(start_close) or start_close == 0:
        return None
    returns_series = ticker_df["daily_return"].dropna()
    if len(returns_series) < MIN_ROWS_FOR_METRICS:
        return None
    period_return = (end_close / start_close) - 1
    annualised_volatility = returns_series.std() * math.sqrt(ANNUALISED_TRADING_DAYS)
    return {
        "ticker": ticker_df.iloc[0]["ticker"],
        "period": period_short_label,
        "return_pct": period_return * 100,
        "volatility_pct": annualised_volatility * 100,
    }


def build_return_volatility_table(dataframe: pd.DataFrame, time_days: int | None, period_short_label: str) -> pd.DataFrame:
    """Compute return and volatility metrics for every ticker in the period."""
    filtered_df = filter_data_for_period(dataframe, time_days)
    results = []
    for ticker in filtered_df["ticker"].unique():
        ticker_df = filtered_df[filtered_df["ticker"] == ticker].copy()
        metric_row = calculate_period_metrics(ticker_df, period_short_label)
        if metric_row is not None:
            results.append(metric_row)
    results_df = pd.DataFrame(results)
    if not results_df.empty:
        results_df = results_df.sort_values(
            "return_pct", ascending=False).reset_index(drop=True)
    return results_df


def get_scope_tickers(metrics_df: pd.DataFrame, scope_label: str) -> list:
    """Return the ticker list for All / Top 10 / Bottom 10 scope options."""
    if metrics_df.empty:
        return []
    if scope_label == "Top 10":
        return metrics_df.sort_values("return_pct", ascending=False).head(NUM_SCOPE_TICKERS)["ticker"].tolist()
    if scope_label == "Bottom 10":
        return metrics_df.sort_values("return_pct", ascending=True).head(NUM_SCOPE_TICKERS)["ticker"].tolist()
    return metrics_df["ticker"].tolist()


def filter_metrics_by_tickers(metrics_df: pd.DataFrame, tickers: list) -> pd.DataFrame:
    """Subset a metrics DataFrame to the given ticker list."""
    if not tickers:
        return pd.DataFrame(columns=metrics_df.columns)
    return metrics_df[metrics_df["ticker"].isin(tickers)].copy().reset_index(drop=True)


# ---------------------------------------------------------------------------
# Return vs Volatility — render
# ---------------------------------------------------------------------------
def render_return_volatility_popover() -> None:
    """Render the info popover explaining the return vs volatility chart."""
    with st.popover("ℹ️"):
        st.markdown(
            "**Return vs Volatility**\n\n"
            "- Each dot is a ticker.\n"
            "- **X-axis** — annualised volatility (σ × √252) over the selected period.\n"
            "- **Y-axis** — total return `(end / start) - 1` over the selected period.\n"
            "- **Colour** — green = positive return, red = negative.\n"
            "- Click a point to highlight that ticker. Double-click to reset."
        )


def render_kpi_metrics(metrics_df: pd.DataFrame) -> None:
    """Render best/worst return and lowest/highest volatility KPI tiles."""
    best_row = metrics_df.sort_values("return_pct", ascending=False).iloc[0]
    worst_row = metrics_df.sort_values("return_pct", ascending=True).iloc[0]
    low_vol_row = metrics_df.sort_values(
        "volatility_pct", ascending=True).iloc[0]
    high_vol_row = metrics_df.sort_values(
        "volatility_pct", ascending=False).iloc[0]

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Best Return", best_row["ticker"],
                "%.2f%%" % best_row["return_pct"])
    kpi2.metric("Worst Return",
                worst_row["ticker"], "%.2f%%" % worst_row["return_pct"])
    kpi3.metric("Lowest Volatility",
                low_vol_row["ticker"], "%.2f%%" % low_vol_row["volatility_pct"])
    kpi4.metric("Highest Volatility",
                high_vol_row["ticker"], "%.2f%%" % high_vol_row["volatility_pct"])


def render_return_volatility_section(metrics_df: pd.DataFrame, period_short_label: str) -> None:
    """Render the full return vs volatility section: header, KPIs, scope radio, chart, caption."""
    title_col, info_col = st.columns([6, 1])
    with title_col:
        st.markdown("#### Return vs Volatility")
    with info_col:
        render_return_volatility_popover()

    render_kpi_metrics(metrics_df)

    scope_label = st.radio(
        "Ticker view",
        LOCAL_SCOPE_OPTIONS,
        horizontal=True,
        key="return_vol_scope",
    )
    scoped_tickers = get_scope_tickers(metrics_df, scope_label)
    scoped_metrics_df = filter_metrics_by_tickers(metrics_df, scoped_tickers)

    st.caption(
        "Hover to see exact values. Click a point to highlight that ticker.")
    chart = create_return_volatility_chart(
        scoped_metrics_df, period_short_label)
    st.altair_chart(chart, use_container_width=True)

    min_ret = round(scoped_metrics_df["return_pct"].abs().min(), 1)
    max_ret = round(scoped_metrics_df["return_pct"].abs().max(), 1)
    st.caption("Bubble size = absolute return %%  ·  smaller (~%s%%) = lower move  ·  larger (~%s%%) = stronger mover" % (
        min_ret, max_ret))
