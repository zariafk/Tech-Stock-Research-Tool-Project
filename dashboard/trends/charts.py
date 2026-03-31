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


# ---------------------------------------------------------------------------
# Return vs Volatility charts
# ---------------------------------------------------------------------------
def build_zero_line() -> alt.Chart:
    """Build the y=0 reference rule for the return vs volatility scatter."""
    return (
        alt.Chart(pd.DataFrame({"y_value": [0]}))
        .mark_rule(strokeDash=[6, 6], color="#9CA3AF")
        .encode(y="y_value:Q")
    )


def build_scatter_points(plot_df: pd.DataFrame, period_short_label: str, selected_ticker) -> alt.Chart:
    """Build scatter points encoded with return, volatility, and colour by return."""
    return (
        alt.Chart(plot_df)
        .mark_circle(stroke="white", strokeWidth=1)
        .encode(
            x=alt.X("volatility_pct:Q", title="%s Volatility (%%" %
                    period_short_label + ")"),
            y=alt.Y("return_pct:Q", title="%s Return (%%" %
                    period_short_label + ")"),
            color=alt.Color(
                "return_pct:Q",
                title="Return %",
                scale=alt.Scale(scheme="redyellowgreen", domainMid=0),
            ),
            opacity=alt.condition(
                selected_ticker, alt.value(1.0), alt.value(0.85)),
            tooltip=[
                alt.Tooltip("ticker:N", title="Ticker"),
                alt.Tooltip("period:N", title="Period"),
                alt.Tooltip("return_pct:Q", title="Return %", format=".2f"),
                alt.Tooltip("volatility_pct:Q",
                            title="Volatility %", format=".2f"),
                alt.Tooltip("abs_return:Q",
                            title="Absolute Return %", format=".2f"),
            ],
        )
        .add_params(selected_ticker)
    )


def build_ticker_labels(plot_df: pd.DataFrame, selected_ticker) -> alt.Chart:
    """Build text labels positioned beside each scatter point."""
    return (
        alt.Chart(plot_df)
        .mark_text(align="left", baseline="middle", dx=8, dy=-6, fontSize=11, color="#E5E7EB")
        .encode(
            x="volatility_pct:Q",
            y="return_pct:Q",
            text="ticker:N",
            opacity=alt.condition(
                selected_ticker, alt.value(1.0), alt.value(0.95)),
        )
    )


def apply_chart_styling(chart: alt.Chart, period_short_label: str) -> alt.Chart:
    """Apply consistent dark-theme axis and title styling to the scatter chart."""
    return (
        chart
        .properties(title="Return vs Volatility (%s)" % period_short_label, height=560)
        .configure_view(strokeOpacity=0)
        .configure_axis(labelColor="#E5E7EB", titleColor="#E5E7EB", gridColor="#253041")
        .configure_legend(labelColor="#E5E7EB", titleColor="#E5E7EB")
        .configure_title(color="#E5E7EB", fontSize=20)
    )


def create_return_volatility_chart(metrics_df: pd.DataFrame, period_short_label: str) -> alt.Chart:
    """Compose the full return vs volatility scatter from sub-layers."""
    if metrics_df.empty:
        return alt.Chart(pd.DataFrame()).mark_circle()

    plot_df = metrics_df.copy()
    plot_df["abs_return"] = plot_df["return_pct"].abs()

    selected_ticker = alt.selection_point(
        fields=["ticker"], on="click", clear="dblclick")
    combined = build_zero_line() + build_scatter_points(plot_df, period_short_label,
                                                        selected_ticker) + build_ticker_labels(plot_df, selected_ticker)
    return apply_chart_styling(combined, period_short_label)
