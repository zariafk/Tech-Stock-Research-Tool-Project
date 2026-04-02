"""
charts.py

Chart building functions for the summary dashboard.
"""

import pandas as pd
import altair as alt


#  Shared chart constants

SENTIMENT_DOMAIN = [-1.2, 1.2]
SENTIMENT_COLOUR_DOMAIN = [-1, 1]
SENTIMENT_COLOUR_SCHEME = "redyellowgreen"
RELEVANCE_COLOUR_SCHEME = "viridis"
BUBBLE_SIZE_RANGE = [40, 600]
RELEVANCE_SIZE_RANGE = [40, 450]
ZERO_RULE_COLOUR = "gray"
ZERO_RULE_DASH = [4, 4]
ZERO_RULE_OPACITY = 0.6
CHART_CIRCLE_OPACITY = 0.75
CHART_CIRCLE_STROKE = "white"
CHART_CIRCLE_STROKE_WIDTH = 0.5
ROLLING_WINDOW = 7
MIN_MOMENTUM_POINTS = 3
POSITIVE_AREA_COLOUR = "#2ecc71"
NEGATIVE_AREA_COLOUR = "#e74c3c"
MIDLINE_COLOUR = "white"
MIDLINE_WIDTH = 1.5
AREA_OPACITY = 0.45
DEFAULT_LINE_COLOUR = "#4A90D9"
TICKER_COLOURS = ["#4A90D9", "#2ecc71"]
INDICATOR_POS_THRESHOLD = 0.1
INDICATOR_NEG_THRESHOLD = -0.1
MARKET_POS_THRESHOLD = 0.01
MARKET_NEG_THRESHOLD = -0.01
SIGNAL_DOMAIN = ["positive", "neutral", "negative"]
SIGNAL_COLORS = [POSITIVE_AREA_COLOUR, "#f39c12", NEGATIVE_AREA_COLOUR]
SIGNAL_SORT_ORDER = ["News", "Reddit", "Market"]
INDICATOR_CIRCLE_SIZE = 2000
INDICATOR_ROW_HEIGHT = 120


#  Shared chart helpers

def _is_multi_ticker(dataframe: pd.DataFrame) -> bool:
    """Check whether a dataframe contains more than one ticker."""
    return "ticker" in dataframe.columns and dataframe["ticker"].nunique() > 1


def _vertical_zero_rule() -> alt.Chart:
    """Dashed vertical rule at x = 0 for sentiment axes."""
    return (
        alt.Chart(pd.DataFrame({"x": [0]}))
        .mark_rule(color=ZERO_RULE_COLOUR, strokeDash=ZERO_RULE_DASH, opacity=ZERO_RULE_OPACITY)
        .encode(x="x:Q")
    )


def _horizontal_zero_rule() -> alt.Chart:
    """Dashed horizontal rule at y = 0 for sentiment axes."""
    return (
        alt.Chart(pd.DataFrame({"y": [0]}))
        .mark_rule(color=ZERO_RULE_COLOUR, strokeDash=ZERO_RULE_DASH, opacity=0.7)
        .encode(y="y:Q")
    )


def _sentiment_x_axis() -> alt.X:
    """Standard sentiment score x-axis with fixed domain."""
    return alt.X(
        "sentiment_score:Q",
        title="Sentiment Score",
        scale=alt.Scale(domain=SENTIMENT_DOMAIN),
    )


def _prepare_social_dates(social: pd.DataFrame) -> pd.DataFrame:
    """Normalise created_at into a date column for merging with price history."""
    social = social.copy()
    social["date"] = pd.to_datetime(social["created_at"]).dt.normalize()
    return social


def _compute_weighted_sentiment(social: pd.DataFrame) -> pd.DataFrame:
    """Add a weighted-sentiment column for momentum calculations."""
    social = social.copy()
    social["weighted"] = social["sentiment_score"] * social["relevance_score"]
    return social


def _ticker_colour_encoding() -> alt.Color:
    """Explicit blue/green colour encoding for multi-ticker comparison."""
    return alt.Color(
        "ticker:N",
        title="Ticker",
        scale=alt.Scale(range=TICKER_COLOURS),
    )


#  Comments vs Sentiment
def _comments_colour_encoding(multi_ticker: bool) -> alt.Color:
    """Colour by ticker in comparison mode, by relevance otherwise."""
    if multi_ticker:
        return _ticker_colour_encoding()
    return alt.Color(
        "relevance_score:Q",
        scale=alt.Scale(scheme=RELEVANCE_COLOUR_SCHEME),
        title="Relevance",
    )


def _comments_tooltip_fields(multi_ticker: bool) -> list:
    """Tooltip list for the comments-vs-sentiment scatter."""
    fields = [
        "title:N",
        alt.Tooltip("sentiment_score:Q", format=".2f"),
        "num_comments:Q",
        "ups:Q",
        alt.Tooltip("relevance_score:Q", format=".2f"),
    ]
    if multi_ticker:
        fields.insert(0, "ticker:N")
    return fields


def build_comments_vs_sentiment_chart(social: pd.DataFrame) -> alt.LayerChart | None:
    """Build comments vs sentiment scatter, with optional ticker comparison."""
    if social.empty:
        return None

    multi_ticker = _is_multi_ticker(social)

    scatter = (
        alt.Chart(social)
        .mark_circle(opacity=CHART_CIRCLE_OPACITY, stroke=CHART_CIRCLE_STROKE,
                     strokeWidth=CHART_CIRCLE_STROKE_WIDTH)
        .encode(
            x=_sentiment_x_axis(),
            y=alt.Y("num_comments:Q", title="Comments"),
            color=_comments_colour_encoding(multi_ticker),
            size=alt.Size("ups:Q", scale=alt.Scale(
                range=BUBBLE_SIZE_RANGE), title="Upvotes"),
            tooltip=_comments_tooltip_fields(multi_ticker),
        )
    )

    return _vertical_zero_rule() + scatter


#  Signal Convergence (Price x Reddit Sentiment)
def _merge_social_with_prices(
    social: pd.DataFrame,
    history: pd.DataFrame,
    multi_ticker: bool,
) -> pd.DataFrame:
    """Inner-join social posts onto daily closing prices."""
    if multi_ticker:
        return social.merge(
            history[["ticker", "bar_date", "close"]],
            left_on=["ticker", "date"],
            right_on=["ticker", "bar_date"],
            how="inner",
        )
    return social.merge(
        history[["bar_date", "close"]],
        left_on="date",
        right_on="bar_date",
        how="inner",
    )


def _convergence_price_line(history: pd.DataFrame, multi_ticker: bool) -> alt.Chart:
    """Price line layer for the convergence chart."""
    colour = _ticker_colour_encoding() if multi_ticker else alt.value(DEFAULT_LINE_COLOUR)
    return (
        alt.Chart(history)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("bar_date:T", title="Date"),
            y=alt.Y("close:Q", title="Price ($)", scale=alt.Scale(zero=False)),
            color=colour,
        )
    )


def _convergence_sentiment_dots(
    social_merged: pd.DataFrame,
    multi_ticker: bool,
    selection: alt.SelectionParameter,
) -> alt.Chart:
    """Sentiment dot layer for the convergence chart."""
    shape = alt.Shape(
        "ticker:N", title="Ticker") if multi_ticker else alt.value("circle")
    ticker_tooltip = ["ticker:N"] if multi_ticker else []

    return (
        alt.Chart(social_merged)
        .mark_circle(stroke=CHART_CIRCLE_STROKE, strokeWidth=CHART_CIRCLE_STROKE_WIDTH)
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("close:Q"),
            size=alt.Size("relevance_score:Q", scale=alt.Scale(
                range=RELEVANCE_SIZE_RANGE), legend=None),
            color=alt.Color(
                "sentiment_score:Q",
                scale=alt.Scale(scheme=SENTIMENT_COLOUR_SCHEME,
                                domain=SENTIMENT_COLOUR_DOMAIN),
                title="Sentiment",
            ),
            shape=shape,
            opacity=alt.condition(selection, alt.value(1.0), alt.value(0.35)),
            tooltip=[
                *ticker_tooltip,
                "title:N",
                alt.Tooltip("sentiment_score:Q", format=".2f"),
                alt.Tooltip("relevance_score:Q", format=".2f"),
                "confidence:N",
            ],
        )
        .add_params(selection)
    )


def build_signal_convergence_chart(
    history: pd.DataFrame,
    social: pd.DataFrame,
) -> tuple[alt.Chart, pd.DataFrame] | tuple[None, None]:
    """Build price line with Reddit sentiment dots, with optional ticker comparison."""
    if history.empty or social.empty:
        return None, None

    history = history.copy()
    history["bar_date"] = pd.to_datetime(history["bar_date"])

    social = _prepare_social_dates(social)
    multi_ticker = _is_multi_ticker(history)

    social_merged = _merge_social_with_prices(social, history, multi_ticker)
    if social_merged.empty:
        return None, None

    selection_fields = ["post_id", "ticker"] if multi_ticker else ["post_id"]
    selection = alt.selection_point(
        fields=selection_fields, name="convergence_sel")

    price_line = _convergence_price_line(history, multi_ticker)
    sentiment_dots = _convergence_sentiment_dots(
        social_merged, multi_ticker, selection)

    chart = (price_line + sentiment_dots).properties(height=350)
    return chart, social_merged


#  Sentiment Momentum (rolling 7-day)

def _aggregate_daily_sentiment(social: pd.DataFrame, multi_ticker: bool) -> pd.DataFrame:
    """Group by date (and ticker) to compute daily weighted sentiment."""
    group_columns = ["ticker", "date"] if multi_ticker else ["date"]
    daily = (
        social.groupby(group_columns)
        .agg(weighted_sum=("weighted", "sum"), relevance_sum=("relevance_score", "sum"))
        .reset_index()
    )
    daily["daily_sentiment"] = daily["weighted_sum"] / \
        daily["relevance_sum"].replace(0, float("nan"))
    return daily


def _apply_rolling_sentiment(daily: pd.DataFrame, multi_ticker: bool) -> pd.DataFrame:
    """Apply a 7-day rolling mean to daily sentiment values."""
    if multi_ticker:
        daily = daily.sort_values(["ticker", "date"])
        frames = []
        for ticker_value in daily["ticker"].unique():
            ticker_df = daily[daily["ticker"] == ticker_value].copy()
            ticker_df["rolling_sentiment"] = ticker_df["daily_sentiment"].rolling(
                window=ROLLING_WINDOW, min_periods=1,
            ).mean()
            frames.append(ticker_df)
        return pd.concat(frames, ignore_index=True)

    daily = daily.sort_values("date")
    daily["rolling_sentiment"] = daily["daily_sentiment"].rolling(
        window=ROLLING_WINDOW, min_periods=1,
    ).mean()
    return daily


def _momentum_multi_ticker_chart(plot_df: pd.DataFrame) -> alt.Chart:
    """Line chart for multi-ticker momentum comparison."""
    return (
        alt.Chart(plot_df)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("rolling_sentiment:Q",
                    title="Weighted Sentiment (7d avg)"),
            color=_ticker_colour_encoding(),
            tooltip=[
                "ticker:N",
                "date:T",
                alt.Tooltip("rolling_sentiment:Q", format=".3f",
                            title="Rolling Sentiment"),
            ],
        )
    )


def _momentum_single_ticker_chart(daily: pd.DataFrame) -> alt.Chart:
    """Area + midline chart for single-ticker momentum."""
    positive_area = (
        alt.Chart(daily)
        .transform_calculate(clipped="max(datum.rolling_sentiment, 0)")
        .mark_area(color=POSITIVE_AREA_COLOUR, opacity=AREA_OPACITY, interpolate="monotone")
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("clipped:Q", title="Weighted Sentiment (7d avg)"),
            y2=alt.Y2(datum=0),
        )
    )

    negative_area = (
        alt.Chart(daily)
        .transform_calculate(clipped="min(datum.rolling_sentiment, 0)")
        .mark_area(color=NEGATIVE_AREA_COLOUR, opacity=AREA_OPACITY, interpolate="monotone")
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("clipped:Q"),
            y2=alt.Y2(datum=0),
        )
    )

    midline = (
        alt.Chart(daily)
        .mark_line(color=MIDLINE_COLOUR, strokeWidth=MIDLINE_WIDTH)
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("rolling_sentiment:Q"),
            tooltip=[
                "date:T",
                alt.Tooltip("rolling_sentiment:Q", format=".3f",
                            title="Rolling Sentiment"),
            ],
        )
    )

    return positive_area + negative_area + midline


def build_sentiment_momentum_chart(social: pd.DataFrame) -> alt.Chart | None:
    """Build rolling 7-day sentiment chart, with optional ticker comparison."""
    if social.empty or len(social) < MIN_MOMENTUM_POINTS:
        return None

    social = _prepare_social_dates(social)
    social = _compute_weighted_sentiment(social)
    multi_ticker = _is_multi_ticker(social)

    daily = _aggregate_daily_sentiment(social, multi_ticker)
    plot_df = _apply_rolling_sentiment(daily, multi_ticker)

    zero_rule = _horizontal_zero_rule()

    if multi_ticker:
        lines = _momentum_multi_ticker_chart(plot_df)
        return (zero_rule + lines).properties(height=220)

    area_chart = _momentum_single_ticker_chart(plot_df)
    return (area_chart + zero_rule).properties(height=220)


#  Engagement Scatter (Upvotes vs Sentiment)
def build_engagement_scatter_chart(social: pd.DataFrame) -> alt.LayerChart | None:
    """Builds scatter plot of upvotes vs sentiment, sized by comment count."""
    if social.empty:
        return None

    scatter = (
        alt.Chart(social)
        .mark_circle(opacity=CHART_CIRCLE_OPACITY, stroke=CHART_CIRCLE_STROKE,
                     strokeWidth=CHART_CIRCLE_STROKE_WIDTH)
        .encode(
            x=_sentiment_x_axis(),
            y=alt.Y("ups:Q", title="Upvotes"),
            color=alt.Color(
                "relevance_score:Q",
                scale=alt.Scale(scheme=RELEVANCE_COLOUR_SCHEME),
                title="Relevance",
            ),
            size=alt.Size("num_comments:Q", scale=alt.Scale(
                range=BUBBLE_SIZE_RANGE), title="Comments"),
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

    return (_vertical_zero_rule() + scatter).properties(height=300)


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


def _weighted_average_score(dataframe: pd.DataFrame) -> float:
    """Returns relevance-weighted average sentiment. Returns 0.0 if no data."""
    if dataframe.empty:
        return 0.0
    total_relevance = dataframe["relevance_score"].sum()
    if total_relevance == 0:
        return 0.0
    return (dataframe["sentiment_score"] * dataframe["relevance_score"]).sum() / total_relevance


def _market_price_change(history: pd.DataFrame) -> float:
    """Returns fractional price change from first to last close.
     Returns 0.0 if insufficient data."""
    if history.empty or len(history) < 2:
        return 0.0
    sorted_history = history.sort_values("bar_date")
    first_close = sorted_history["close"].iloc[0]
    last_close = sorted_history["close"].iloc[-1]
    if first_close == 0:
        return 0.0
    return (last_close - first_close) / first_close


def _build_single_ticker_indicators(
    news: pd.DataFrame,
    social: pd.DataFrame,
    history: pd.DataFrame,
) -> pd.DataFrame:
    """Build indicator dataframe for a single ticker."""
    news_score = _weighted_average_score(news)
    reddit_score = _weighted_average_score(social)
    market_change = _market_price_change(history)

    return pd.DataFrame({
        "label": SIGNAL_SORT_ORDER,
        "signal": [
            _classify_signal(news_score),
            _classify_signal(reddit_score),
            _market_classify(market_change),
        ],
        "score": [round(news_score, 3), round(reddit_score, 3), round(market_change, 4)],
        "y": [0, 0, 0],
    })


def _single_ticker_indicator_chart(indicators: pd.DataFrame) -> alt.Chart:
    """Render circle + text indicator row for a single ticker."""
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


def _build_multi_ticker_indicators(
    news: pd.DataFrame,
    social: pd.DataFrame,
    history: pd.DataFrame,
) -> pd.DataFrame:
    """Build indicator dataframe for multiple tickers."""
    tickers = sorted(history["ticker"].dropna().unique().tolist())
    rows = []

    for ticker in tickers:
        news_slice = news[news["ticker"] == ticker]
        social_slice = social[social["ticker"] == ticker]
        history_slice = history[history["ticker"] == ticker]

        news_score = _weighted_average_score(news_slice)
        reddit_score = _weighted_average_score(social_slice)
        market_change = _market_price_change(history_slice)

        rows.extend([
            {"ticker": ticker, "label": "News", "signal": _classify_signal(
                news_score), "score": round(news_score, 3)},
            {"ticker": ticker, "label": "Reddit", "signal": _classify_signal(
                reddit_score), "score": round(reddit_score, 3)},
            {"ticker": ticker, "label": "Market", "signal": _market_classify(
                market_change), "score": round(market_change, 4)},
        ])

    return pd.DataFrame(rows)


def _multi_ticker_indicator_chart(indicators: pd.DataFrame) -> alt.Chart:
    """Render circle grid for multi-ticker comparison."""
    return (
        alt.Chart(indicators)
        .mark_circle(size=1300)
        .encode(
            x=alt.X(
                "label:N",
                sort=SIGNAL_SORT_ORDER,
                title=None,
                axis=alt.Axis(labelAngle=0, labelFontSize=14,
                              labelFontWeight="bold"),
            ),
            y=alt.Y(
                "ticker:N",
                title=None,
                axis=alt.Axis(labelFontSize=13, labelFontWeight="bold"),
            ),
            color=alt.Color(
                "signal:N",
                scale=alt.Scale(domain=SIGNAL_DOMAIN, range=SIGNAL_COLORS),
                legend=None,
            ),
            tooltip=[
                "ticker:N",
                alt.Tooltip("label:N", title="Source"),
                alt.Tooltip("signal:N", title="Signal"),
                alt.Tooltip("score:Q", format="+.3f", title="Score"),
            ],
        )
        .properties(height=160)
    )


def build_sentiment_indicator_row(
    news: pd.DataFrame,
    social: pd.DataFrame,
    history: pd.DataFrame,
) -> alt.Chart:
    """Build source sentiment indicators, with optional ticker comparison."""
    multi_ticker = _is_multi_ticker(history)

    if not multi_ticker:
        indicators = _build_single_ticker_indicators(news, social, history)
        return _single_ticker_indicator_chart(indicators)

    indicators = _build_multi_ticker_indicators(news, social, history)
    return _multi_ticker_indicator_chart(indicators)
