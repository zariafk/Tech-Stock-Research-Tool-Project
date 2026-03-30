"""
charts.py

Chart building functions for the trends dashboard.
"""

import pandas as pd
import altair as alt


def build_stacked_bar_chart(avg_per: pd.DataFrame, avg_per_filtered: pd.DataFrame) -> alt.Chart:
    """
    Builds a stacked bar chart comparing relative volume and trade count.

    Args:
        avg_per (pd.DataFrame): Aggregated market data with relative volume and trade counts.
        avg_per_filtered (pd.DataFrame): Filtered market data for selected tickers.

    Returns:
        alt.Chart: Altair chart object.
    """
    vol_max = avg_per_filtered["relative_volume"].max()
    tc_max = avg_per_filtered["avg_trade_count"].max()
    stacked = pd.concat([
        avg_per_filtered[["ticker", "relative_volume"]].rename(
            columns={"relative_volume": "value"}
        ).assign(
            metric="Relative Volume",
            value_norm=avg_per_filtered["relative_volume"] / vol_max
        ),
        avg_per_filtered[["ticker", "avg_trade_count"]].rename(
            columns={"avg_trade_count": "value"}
        ).assign(
            metric="Trade Count",
            value_norm=avg_per_filtered["avg_trade_count"] / tc_max
        ),
    ], ignore_index=True)

    return (
        alt.Chart(stacked)
        .mark_bar()
        .encode(
            x=alt.X("ticker:N", title="Ticker",
                    axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("value_norm:Q",
                    title="Normalised Value (0–1)", stack="zero"),
            color=alt.Color("metric:N", legend=alt.Legend(title="Metric")),
            tooltip=["ticker:N", "metric:N", alt.Tooltip("value:Q", format=".2f"),
                     alt.Tooltip("value_norm:Q", format=".2f", title="Normalised")],
        )
        .properties(height=260)
    )


def build_price_line_chart(df_line: pd.DataFrame) -> alt.Chart:
    """
    Builds a line chart for closing price over time with legend interaction.

    Args:
        df_line (pd.DataFrame): Market data filtered by selected tickers.

    Returns:
        alt.Chart: Altair chart object.
    """
    highlight = alt.selection_point(fields=["ticker"], bind="legend")

    return (
        alt.Chart(df_line)
        .mark_line()
        .encode(
            x=alt.X("bar_date:T", title="Date"),
            y=alt.Y("close:Q", title="Close Price (USD)",
                    scale=alt.Scale(zero=False)),
            color=alt.Color("ticker:N", legend=alt.Legend(title="Ticker")),
            opacity=alt.condition(highlight, alt.value(1), alt.value(0.15)),
            tooltip=["ticker:N", "bar_date:T", alt.Tooltip(
                "close:Q", format=".2f"), alt.Tooltip("volume:Q", format=",")],
        )
        .add_params(highlight)
        .properties(height=320)
    )


def build_sentiment_lollipop_chart(df_combined: pd.DataFrame) -> alt.Chart:
    """
    Builds a lollipop chart for average sentiment by ticker.

    Args:
        df_combined (pd.DataFrame): Sentiment data with direction (Positive/Negative).

    Returns:
        alt.Chart: Altair chart object.
    """
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

    return (combined_rule + combined_point).properties(height=260)
