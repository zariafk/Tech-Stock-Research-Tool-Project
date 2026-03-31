"""
charts.py

Chart building functions for the summary dashboard.
"""

import pandas as pd
import altair as alt


# def build_comments_vs_sentiment_chart(social: pd.DataFrame) -> alt.LayerChart | None:
#     """
#     Builds a scatter plot of comments vs sentiment with relevance as color.

#     Args:
#         social (pd.DataFrame): Reddit data with sentiment, relevance, and engagement metrics.

#     Returns:
#         alt.LayerChart | None: Altair chart object or None if data is empty.
#     """
#     if social.empty:
#         return None

#     zero_rule = (
#         alt.Chart(pd.DataFrame({"x": [0]}))
#         .mark_rule(color="gray", strokeDash=[4, 4], opacity=0.6)
#         .encode(x="x:Q")
#     )

#     scatter = (
#         alt.Chart(social)
#         .mark_circle(opacity=0.75, stroke="white", strokeWidth=0.5)
#         .encode(
#             x=alt.X("sentiment_score:Q", title="Sentiment Score",
#                     scale=alt.Scale(domain=[-1.2, 1.2])),
#             y=alt.Y("num_comments:Q", title="Comments"),
#             color=alt.Color("relevance_score:Q", scale=alt.Scale(
#                 scheme="viridis"), title="Relevance"),
#             size=alt.Size("ups:Q", scale=alt.Scale(
#                 range=[40, 600]), title="Upvotes"),
#             tooltip=[
#                 "title:N",
#                 alt.Tooltip("sentiment_score:Q", format=".2f"),
#                 "num_comments:Q",
#                 "ups:Q",
#                 alt.Tooltip("relevance_score:Q", format=".2f"),
#             ],
#         )
#     )

#     return (zero_rule + scatter)


def build_signal_convergence_chart(history: pd.DataFrame, social: pd.DataFrame) -> tuple[alt.Chart, pd.DataFrame] | tuple[None, None]:
    """
    Builds price line overlaid with Reddit sentiment dots.

    Args:
        history (pd.DataFrame): Market price history data.
        social (pd.DataFrame): Reddit data with sentiment and relevance scores.

    Returns:
        tuple[alt.Chart, pd.DataFrame] | tuple[None, None]: Chart and merged data or (None, None).
    """
    if history.empty or social.empty:
        return None, None

    history = history.copy()
    history["bar_date"] = pd.to_datetime(history["bar_date"])

    social = social.copy()
    social["date"] = pd.to_datetime(social["created_at"]).dt.normalize()
    social_merged = social.merge(
        history[["bar_date", "close"]], left_on="date", right_on="bar_date", how="inner"
    )

    if history.empty or social_merged.empty:
        return None, None

    selection = alt.selection_point(fields=["post_id"], name="convergence_sel")

    price_line = (
        alt.Chart(history)
        .mark_line(strokeWidth=2, color="#4A90D9")
        .encode(
            x=alt.X("bar_date:T", title="Date"),
            y=alt.Y("close:Q", title="Price ($)", scale=alt.Scale(zero=False)),
        )
    )

    sentiment_dots = (
        alt.Chart(social_merged)
        .mark_circle(stroke="white", strokeWidth=0.5)
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("close:Q"),
            size=alt.Size("relevance_score:Q", scale=alt.Scale(
                range=[40, 450]), title="Relevance"),
            color=alt.Color(
                "sentiment_score:Q",
                scale=alt.Scale(scheme="redyellowgreen", domain=[-1, 1]),
                title="Sentiment",
            ),
            opacity=alt.condition(selection, alt.value(1.0), alt.value(0.35)),
            tooltip=[
                "title:N",
                alt.Tooltip("sentiment_score:Q", format=".2f"),
                alt.Tooltip("relevance_score:Q", format=".2f"),
                "confidence:N",
            ],
        )
        .add_params(selection)
    )

    volume_bar = (
        alt.Chart(history)
        .mark_bar(color="#cbd5e0")
        .encode(
            x=alt.X("bar_date:T", axis=None),
            y=alt.Y("volume:Q", title=None)
        )
        .properties(height=60)
    )

    chart = (price_line + sentiment_dots).properties(height=350)
    # chart = alt.concat(main_chart, volume_bar).resolve_scale(x='shared')
    return chart, social_merged


def build_sentiment_momentum_chart(social: pd.DataFrame) -> alt.LayerChart | None:
    """Builds a rolling 7-day sentiment area chart weighted by relevance score."""
    if social.empty or len(social) < 3:
        return None

    daily = social.copy()
    daily["date"] = pd.to_datetime(daily["created_at"]).dt.normalize()
    daily["weighted"] = daily["sentiment_score"] * daily["relevance_score"]
    daily = (
        daily.groupby("date")
        .agg(weighted_sum=("weighted", "sum"), relevance_sum=("relevance_score", "sum"))
        .reset_index()
    )
    daily["daily_sentiment"] = daily["weighted_sum"] / \
        daily["relevance_sum"].replace(0, float("nan"))
    daily = daily.sort_values("date")
    daily["rolling_sentiment"] = daily["daily_sentiment"].rolling(
        window=7, min_periods=1).mean()

    positive_area = (
        alt.Chart(daily)
        .transform_calculate(clipped="max(datum.rolling_sentiment, 0)")
        .mark_area(color="#2ecc71", opacity=0.45, interpolate="monotone")
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("clipped:Q", title="Weighted Sentiment (7d avg)"),
            y2=alt.Y2(datum=0),
        )
    )

    negative_area = (
        alt.Chart(daily)
        .transform_calculate(clipped="min(datum.rolling_sentiment, 0)")
        .mark_area(color="#e74c3c", opacity=0.45, interpolate="monotone")
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("clipped:Q"),
            y2=alt.Y2(datum=0),
        )
    )

    midline = (
        alt.Chart(daily)
        .mark_line(color="white", strokeWidth=1.5)
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("rolling_sentiment:Q"),
            tooltip=["date:T", alt.Tooltip(
                "rolling_sentiment:Q", format=".3f", title="Rolling Sentiment")],
        )
    )

    zero_rule = (
        alt.Chart(pd.DataFrame({"y": [0]}))
        .mark_rule(color="gray", strokeDash=[4, 4], opacity=0.7)
        .encode(y="y:Q")
    )

    return (positive_area + negative_area + midline + zero_rule).properties(height=220)


def build_engagement_scatter_chart(social: pd.DataFrame) -> alt.LayerChart | None:
    """Builds scatter plot of upvotes vs sentiment, sized by comment count."""
    if social.empty:
        return None

    zero_rule = (
        alt.Chart(pd.DataFrame({"x": [0]}))
        .mark_rule(color="gray", strokeDash=[4, 4], opacity=0.6)
        .encode(x="x:Q")
    )

    scatter = (
        alt.Chart(social)
        .mark_circle(opacity=0.75, stroke="white", strokeWidth=0.5)
        .encode(
            x=alt.X("sentiment_score:Q", title="Sentiment Score",
                    scale=alt.Scale(domain=[-1.2, 1.2])),
            y=alt.Y("ups:Q", title="Upvotes"),
            color=alt.Color("relevance_score:Q", scale=alt.Scale(
                scheme="viridis"), title="Relevance"),
            size=alt.Size("num_comments:Q", scale=alt.Scale(
                range=[40, 600]), title="Comments"),
            tooltip=[
                "title:N",
                alt.Tooltip("sentiment_score:Q", format=".2f"),
                "ups:Q",
                "num_comments:Q",
                alt.Tooltip("relevance_score:Q", format=".2f"),
                "confidence:N",
            ],
        )
    )

    return (zero_rule + scatter).properties(height=300)


# ---------------------------------------------------------------------------
#  Classifying sentiment overview for each source
# ---------------------------------------------------------------------------
INDICATOR_POS_THRESHOLD = 0.1
INDICATOR_NEG_THRESHOLD = -0.1
MARKET_POS_THRESHOLD = 0.01
MARKET_NEG_THRESHOLD = -0.01
SIGNAL_DOMAIN = ["positive", "neutral", "negative"]
SIGNAL_COLORS = ["#2ecc71", "#f39c12", "#e74c3c"]
SIGNAL_SORT_ORDER = ["News", "Reddit", "Market"]
INDICATOR_CIRCLE_SIZE = 2000
INDICATOR_ROW_HEIGHT = 120


def _classify_signal(score: float) -> str:
    """Returns 'positive', 'neutral', or 'negative' for sentiment thresholds."""
    if score > INDICATOR_POS_THRESHOLD:
        return "positive"
    if score < INDICATOR_NEG_THRESHOLD:
        return "negative"
    return "neutral"


def _market_classify(change: float) -> str:
    """Returns 'positive', 'neutral', or 'negative' using market-specific thresholds."""
    if change > MARKET_POS_THRESHOLD:
        return "positive"
    if change < MARKET_NEG_THRESHOLD:
        return "negative"
    return "neutral"


def _news_aggregate_score(news: pd.DataFrame) -> float:
    """Returns relevance-weighted average news sentiment. Returns 0.0 if no data."""
    if news.empty:
        return 0.0
    total_relevance = news["relevance_score"].sum()
    if total_relevance == 0:
        return 0.0
    return (news["sentiment_score"] * news["relevance_score"]).sum() / total_relevance


def _reddit_aggregate_score(social: pd.DataFrame) -> float:
    """Returns relevance-weighted average Reddit sentiment. Returns 0.0 if no data."""
    if social.empty:
        return 0.0
    total_relevance = social["relevance_score"].sum()
    if total_relevance == 0:
        return 0.0
    return (social["sentiment_score"] * social["relevance_score"]).sum() / total_relevance


def _market_price_change(history: pd.DataFrame) -> float:
    """Returns fractional price change from first to last close. Returns 0.0 if insufficient data."""
    if history.empty or len(history) < 2:
        return 0.0
    sorted_history = history.sort_values("bar_date")
    first_close = sorted_history["close"].iloc[0]
    last_close = sorted_history["close"].iloc[-1]
    if first_close == 0:
        return 0.0
    return (last_close - first_close) / first_close


def build_sentiment_indicator_row(
    news: pd.DataFrame, social: pd.DataFrame, history: pd.DataFrame
) -> alt.LayerChart:
    """Builds a compact three-indicator row: News · Reddit · Market, each coloured green/amber/red."""
    news_score = _news_aggregate_score(news)
    reddit_score = _reddit_aggregate_score(social)
    market_change = _market_price_change(history)

    indicators = pd.DataFrame({
        "label": SIGNAL_SORT_ORDER,
        "signal": [
            _classify_signal(news_score),
            _classify_signal(reddit_score),
            _market_classify(market_change),
        ],
        "score": [round(news_score, 3), round(reddit_score, 3), round(market_change, 4)],
        "y": [0, 0, 0],
    })

    shared_x = alt.X(
        "label:N",
        title=None,
        sort=SIGNAL_SORT_ORDER,
        axis=alt.Axis(labelAngle=0, ticks=False, domain=False,
                      labelFontSize=15, labelFontWeight="bold"),
    )
    shared_color = alt.Color(
        "signal:N",
        scale=alt.Scale(domain=SIGNAL_DOMAIN, range=SIGNAL_COLORS),
        legend=None,
    )
    shared_y = alt.Y("y:Q", axis=None, scale=alt.Scale(domain=[-1, 1]))

    circles = (
        alt.Chart(indicators)
        .mark_circle(size=INDICATOR_CIRCLE_SIZE)
        .encode(
            x=shared_x,
            y=shared_y,
            color=shared_color,
            tooltip=[
                alt.Tooltip("label:N", title="Source"),
                alt.Tooltip("signal:N", title="Signal"),
                alt.Tooltip("score:Q", format="+.3f", title="Score"),
            ],
        )
    )

    signal_text = (
        alt.Chart(indicators)
        .mark_text(dy=38, fontSize=12)
        .encode(
            x=alt.X("label:N", sort=SIGNAL_SORT_ORDER),
            y=shared_y,
            text=alt.Text("signal:N"),
            color=shared_color,
        )
    )

    return (circles + signal_text).properties(height=INDICATOR_ROW_HEIGHT)
