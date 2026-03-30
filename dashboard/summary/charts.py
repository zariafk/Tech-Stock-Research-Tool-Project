"""
charts.py

Chart building functions for the summary dashboard.
"""

import pandas as pd
import altair as alt


def build_comments_vs_sentiment_chart(social: pd.DataFrame) -> alt.LayerChart | None:
    """
    Builds a scatter plot of comments vs sentiment with relevance as color.

    Args:
        social (pd.DataFrame): Reddit data with sentiment, relevance, and engagement metrics.

    Returns:
        alt.LayerChart | None: Altair chart object or None if data is empty.
    """
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
            y=alt.Y("num_comments:Q", title="Comments"),
            color=alt.Color("relevance_score:Q", scale=alt.Scale(
                scheme="viridis"), title="Relevance"),
            size=alt.Size("ups:Q", scale=alt.Scale(
                range=[40, 600]), title="Upvotes"),
            tooltip=[
                "title:N",
                alt.Tooltip("sentiment_score:Q", format=".2f"),
                "num_comments:Q",
                "ups:Q",
                alt.Tooltip("relevance_score:Q", format=".2f"),
            ],
        )
    )

    return (zero_rule + scatter)


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

    main_chart = (price_line + sentiment_dots).properties(height=350)
    chart = alt.vconcat(main_chart, volume_bar).resolve_scale(x='shared')
    return chart, social_merged


def build_sentiment_momentum_chart(social: pd.DataFrame) -> alt.LayerChart | None:
    """
    Builds a rolling 7-day sentiment momentum area chart.

    Args:
        social (pd.DataFrame): Reddit data with sentiment and relevance scores.

    Returns:
        alt.LayerChart | None: Altair chart object or None if insufficient data.
    """
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
    """
    Builds scatter plot of upvotes vs sentiment with relevance as color.

    Args:
        social (pd.DataFrame): Reddit data with sentiment, relevance, and engagement metrics.

    Returns:
        alt.LayerChart | None: Altair chart object or None if data is empty.
    """
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


def build_news_horizon_chart(news: pd.DataFrame) -> alt.Chart | None:
    """
    Builds a strip plot showing news coverage density by source over time.

    Args:
        news (pd.DataFrame): News articles with publication dates, relevance, and source.

    Returns:
        alt.Chart | None: Altair chart object or None if data is empty.
    """
    if news.empty:
        return None

    news = news.copy()
    news["published_date"] = pd.to_datetime(news["published_date"])
    chart_height = max(200, news["source"].nunique() * 32)

    return (
        alt.Chart(news)
        .mark_circle(size=90)
        .encode(
            x=alt.X("published_date:T", title="Published Date"),
            y=alt.Y("source:N", title="Source", sort="-x"),
            color=alt.Color("relevance_score:Q", scale=alt.Scale(
                scheme="orangered"), title="Relevance"),
            opacity=alt.Opacity("relevance_score:Q",
                                scale=alt.Scale(range=[0.15, 1.0])),
            tooltip=[
                "title:N",
                alt.Tooltip("relevance_score:Q", format=".2f"),
                alt.Tooltip("sentiment_score:Q", format=".2f"),
                "published_date:T",
                "confidence:N",
            ],
        )
        .properties(height=chart_height)
    )


def classify_sentiment(score: float | None) -> tuple[str, str]:
    """
    Converts sentiment score to plain English classification and emoji.

    Args:
        score (float | None): Sentiment score between -1 and 1.

    Returns:
        tuple[str, str]: Classification label and color.
    """
    if score is None:
        return "Unknown", "gray"
    if score > 0.5:
        return "🟢 Positive", "green"
    elif score < -0.5:
        return "🔴 Negative", "red"
    else:
        return "🟡 Neutral", "orange"


def format_price(val: float | None) -> str:
    """
    Formats price with 2 decimals.

    Args:
        val (float | None): Price value to format.

    Returns:
        str: Formatted price string.
    """
    return f"${val:,.2f}" if val else "N/A"

# AI SLOP::::::::::::::!


# ---------------------------------------------------------------------------
# Technical Analysis — Computation
# ---------------------------------------------------------------------------
def compute_rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    """Compute Relative Strength Index from a close price series."""
    delta = closes.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.rolling(window=period, min_periods=period).mean()
    avg_loss = losses.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def compute_bollinger_bands(closes: pd.Series, window: int = 20, num_std: float = 2.0) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Compute Bollinger Bands (middle, upper, lower) from close prices."""
    middle = closes.rolling(window=window).mean()
    std_dev = closes.rolling(window=window).std()
    upper = middle + (std_dev * num_std)
    lower = middle - (std_dev * num_std)
    return middle, upper, lower


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add SMA, RSI, Bollinger Band, and volatility columns to price history."""
    df = df.copy().sort_values("bar_date")
    df["sma_20"] = df["close"].rolling(20).mean()
    df["sma_50"] = df["close"].rolling(50).mean()
    df["rsi"] = compute_rsi(df["close"])
    bb_middle, bb_upper, bb_lower = compute_bollinger_bands(df["close"])
    df["bb_upper"] = bb_upper
    df["bb_lower"] = bb_lower
    df["bb_middle"] = bb_middle
    df["daily_return"] = df["close"].pct_change()
    df["volatility_20d"] = df["daily_return"].rolling(20).std()
    return df


def classify_volume(ratio: float) -> str:
    """Classify volume ratio as Normal or Unusual (above 2x average)."""
    unusual_threshold = 2.0
    if ratio > unusual_threshold:
        return "Unusual"
    return "Normal"


# ---------------------------------------------------------------------------
# Technical Analysis — Charts
# ---------------------------------------------------------------------------
def build_bollinger_chart(df: pd.DataFrame) -> alt.LayerChart | None:
    """Build price chart with Bollinger Bands and SMA20/SMA50 overlay lines."""
    min_periods = 20
    if len(df) < min_periods:
        return None
    band = alt.Chart(df).mark_area(opacity=0.1, color="#4A90D9").encode(
        x="bar_date:T", y="bb_upper:Q", y2="bb_lower:Q"
    )
    price = alt.Chart(df).mark_line(color="#4A90D9", strokeWidth=2).encode(
        x=alt.X("bar_date:T", title="Date"),
        y=alt.Y("close:Q", title="Price ($)", scale=alt.Scale(zero=False)),
        tooltip=["bar_date:T", alt.Tooltip(
            "close:Q", format="$.2f"), alt.Tooltip("sma_20:Q", format="$.2f")]
    )
    sma20 = alt.Chart(df).mark_line(color="#f39c12", strokeWidth=1.5, strokeDash=[
        4, 2]).encode(x="bar_date:T", y="sma_20:Q")
    sma50 = alt.Chart(df).mark_line(color="#e74c3c", strokeWidth=1.5, strokeDash=[
        6, 3]).encode(x="bar_date:T", y="sma_50:Q")
    return (band + price + sma20 + sma50).properties(height=300)


def build_rsi_chart(df: pd.DataFrame) -> alt.LayerChart | None:
    """Build RSI chart with overbought (70) and oversold (30) threshold lines."""
    rsi_overbought = 70
    rsi_oversold = 30
    if "rsi" not in df.columns or df["rsi"].dropna().empty:
        return None
    overbought = alt.Chart(pd.DataFrame({"y": [rsi_overbought]})).mark_rule(
        color="#e74c3c", strokeDash=[4, 4]).encode(y="y:Q")
    oversold = alt.Chart(pd.DataFrame({"y": [rsi_oversold]})).mark_rule(
        color="#2ecc71", strokeDash=[4, 4]).encode(y="y:Q")
    neutral = alt.Chart(pd.DataFrame({"y": [50]})).mark_rule(
        color="gray", strokeDash=[2, 2], opacity=0.4).encode(y="y:Q")
    rsi_line = alt.Chart(df).mark_line(color="#9b59b6", strokeWidth=1.5).encode(
        x=alt.X("bar_date:T", title="Date"),
        y=alt.Y("rsi:Q", title="RSI (14)", scale=alt.Scale(domain=[0, 100])),
        tooltip=["bar_date:T", alt.Tooltip("rsi:Q", format=".1f")]
    )
    return (rsi_line + overbought + oversold + neutral).properties(height=150)


def build_volume_analysis_chart(df: pd.DataFrame) -> alt.LayerChart | None:
    """Build volume bar chart with 20-day average and anomaly highlighting."""
    min_periods = 20
    if df.empty or len(df) < min_periods:
        return None
    df = df.copy()
    df["vol_avg_20"] = df["volume"].rolling(min_periods).mean()
    df["vol_ratio"] = df["volume"] / df["vol_avg_20"]
    df["vol_signal"] = df["vol_ratio"].apply(classify_volume)
    bars = alt.Chart(df).mark_bar().encode(
        x=alt.X("bar_date:T", title="Date"),
        y=alt.Y("volume:Q", title="Volume"),
        color=alt.Color("vol_signal:N", scale=alt.Scale(
            domain=["Normal", "Unusual"], range=["#cbd5e0", "#e74c3c"]), title="Activity"),
        tooltip=["bar_date:T", "volume:Q", alt.Tooltip(
            "vol_ratio:Q", format=".1f", title="Vol Ratio")]
    )
    avg_line = alt.Chart(df).mark_line(color="#f39c12", strokeWidth=2, strokeDash=[
        6, 3]).encode(x="bar_date:T", y="vol_avg_20:Q")
    return (bars + avg_line).properties(height=150)


# ---------------------------------------------------------------------------
# Risk & Composite Signal
# ---------------------------------------------------------------------------
def compute_risk_metrics(df: pd.DataFrame) -> dict:
    """Compute max drawdown, annualised volatility, and Sharpe ratio from price history."""
    min_trading_days = 10
    closes = df["close"].dropna()
    if len(closes) < min_trading_days:
        return {}
    daily_returns = closes.pct_change().dropna()
    cumulative_max = closes.cummax()
    drawdowns = (closes - cumulative_max) / cumulative_max
    annual_trading_days = 252
    risk_free_annual = 0.05
    risk_free_daily = risk_free_annual / annual_trading_days
    annual_vol = daily_returns.std() * (annual_trading_days ** 0.5)
    excess_returns = daily_returns - risk_free_daily
    sharpe = (excess_returns.mean() / daily_returns.std()) * \
        (annual_trading_days ** 0.5) if daily_returns.std() > 0 else 0
    return {
        "max_drawdown": round(drawdowns.min() * 100, 2),
        "annual_volatility": round(annual_vol * 100, 2),
        "sharpe_ratio": round(sharpe, 2),
        "trading_days": len(daily_returns),
    }


def normalize_rsi_signal(rsi: float | None) -> float:
    """Normalize RSI (0-100) to signal range (-1 to +1), where 50 is neutral."""
    if rsi is None:
        return 0.0
    rsi_neutral = 50
    return (rsi - rsi_neutral) / rsi_neutral


def interpret_composite_score(score: float) -> tuple[str, str]:
    """Classify composite score into a label and colour for display."""
    strong_threshold = 0.3
    mild_threshold = 0.1
    if score > strong_threshold:
        return "Strong Bullish", "green"
    if score > mild_threshold:
        return "Mildly Bullish", "green"
    if score < -strong_threshold:
        return "Strong Bearish", "red"
    if score < -mild_threshold:
        return "Mildly Bearish", "red"
    return "Neutral", "orange"


def compute_composite_signal(news_sent: float, social_sent: float, rsi: float | None, price_vs_sma: float | None) -> dict:
    """Calculate weighted composite score aggregating all signal sources."""
    news_weight = 0.30
    social_weight = 0.25
    technical_weight = 0.25
    momentum_weight = 0.20
    rsi_signal = normalize_rsi_signal(rsi)
    sma_signal = max(min(price_vs_sma or 0, 1), -1)
    composite = (news_sent * news_weight + social_sent * social_weight +
                 rsi_signal * technical_weight + sma_signal * momentum_weight)
    label, color = interpret_composite_score(composite)
    return {
        "score": round(composite, 3),
        "label": label,
        "color": color,
        "news_contrib": round(news_sent * news_weight, 3),
        "social_contrib": round(social_sent * social_weight, 3),
        "technical_contrib": round(rsi_signal * technical_weight, 3),
        "momentum_contrib": round(sma_signal * momentum_weight, 3),
    }
