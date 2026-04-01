"""
helpers.py

Display utilities and section render functions for the summary dashboard.
"""

import streamlit as st
import pandas as pd
from .charts import (
    build_signal_convergence_chart,
    build_sentiment_momentum_chart,
    build_engagement_scatter_chart,
    build_sentiment_indicator_row,
    build_comments_vs_sentiment_chart
)
import os
import requests

TIME_OPTIONS: dict[str, int | None] = {
    "1 Month":    30,
    "3 Months":   90,
    "6 Months":   180,
    "1 Year":     365,
    "From Start": None,
}

CONFIDENCE_EMOJI: dict[str, str] = {
    "High": "✅",
    "Medium": "⚠️",
    "Low": "❌",
    "Unknown": "❓",
}

NEWS_CARD_HEIGHT_PX = 110
NEWS_CARDS_VISIBLE = 3
TOP_DISCUSSIONS_LIMIT = 5
SENTIMENT_POSITIVE_THRESHOLD = 0.5
SENTIMENT_NEGATIVE_THRESHOLD = -0.5
ARTICLE_COLOUR_POS_THRESHOLD = 0.2
ARTICLE_COLOUR_NEG_THRESHOLD = -0.2
HIGH_DIVERGENCE_THRESHOLD = 0.5
DIVERGENCE_ALIGNED_THRESHOLD = 0.3

RAG_API_URL = os.environ["RAG_API_URL"]


# ---------------------------------------------------------------------------
#  Formatting helpers
# ---------------------------------------------------------------------------
def classify_sentiment(score: float | None) -> tuple[str, str]:
    """Converts a -1 to +1 sentiment score into a label and colour string."""
    if score is None:
        return "Unknown", "gray"
    if score > SENTIMENT_POSITIVE_THRESHOLD:
        return "🟢 Positive", "green"
    if score < SENTIMENT_NEGATIVE_THRESHOLD:
        return "🔴 Negative", "red"
    return "🟡 Neutral", "orange"


def format_price(val: float | None) -> str:
    """Formats a numeric price as a USD string with 2 decimal places."""
    return f"${val:,.2f}" if val else "N/A"


def format_volume(value: float | None) -> str:
    """Format volume in a readable way."""
    if value is None or pd.isna(value) or value == 0:
        return "N/A"
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{int(value):,}"


# ---------------------------------------------------------------------------
#  Market section
# ---------------------------------------------------------------------------
def summarise_history(history: pd.DataFrame) -> dict:
    """Return selected-period summary stats from filtered history."""
    if history.empty:
        return {
            "period_low": None,
            "period_high": None,
            "period_volume": None,
            "period_start_open": None,
            "trend_msg": None,
        }

    history = history.copy()
    history["bar_date"] = pd.to_datetime(history["bar_date"])
    history = history.sort_values("bar_date")

    trend_msg = _compute_trend_message(history)

    return {
        "period_low": history["low"].min(),
        "period_high": history["high"].max(),
        "period_volume": history["volume"].sum(),
        "period_start_open": history.iloc[0]["open"],
        "trend_msg": trend_msg,
    }


def _compute_trend_message(history: pd.DataFrame) -> str | None:
    """Derive a short trend label from the last 5 closing prices."""
    minimum_trend_points = 5
    if len(history) < minimum_trend_points:
        return None

    recent_closes = history["close"].tail(minimum_trend_points).tolist()
    if recent_closes[-1] > recent_closes[0]:
        return "**Uptrend.** Recent prices are rising."
    if recent_closes[-1] < recent_closes[0]:
        return "**Downtrend.** Recent prices are falling."
    return "**Sideways.** Prices are stable."


def build_period_caption(
    price: float,
    period_start_open: float | None,
    time_label: str,
) -> tuple[str, float | None]:
    """Return comparison caption and absolute change versus selected period start."""
    if period_start_open is None or pd.isna(period_start_open) or period_start_open == 0:
        return "📈 Live price comparison versus the selected period start is unavailable.", None

    period_change = price - period_start_open
    period_change_pct = (period_change / period_start_open) * 100
    comparison_label = (
        "the selected period start"
        if time_label == "From Start"
        else f"the {time_label.lower()} opening level"
    )

    if period_change_pct > 0:
        caption = f"📈 Live price is up {abs(period_change_pct):.2f}% versus {comparison_label}."
    elif period_change_pct < 0:
        caption = f"📉 Live price is down {abs(period_change_pct):.2f}% versus {comparison_label}."
    else:
        caption = f"➖ Live price is unchanged versus {comparison_label}."

    return caption, period_change


def _render_price_metric(price: float, period_change: float | None) -> None:
    """Display the current price metric card."""
    st.metric(
        "Current Price",
        format_price(price),
        f"{period_change:+.2f}" if period_change is not None else None,
    )


def _render_range_metric(
    time_label: str,
    period_low: float | None,
    period_high: float | None,
) -> None:
    """Display the period price range metric card."""
    if period_low is not None and period_high is not None:
        low_text = format_price(period_low).replace("$", r"\$")
        high_text = format_price(period_high).replace("$", r"\$")
        spread_text = format_price(
            period_high - period_low).replace("$", r"\$")
        st.metric(f"{time_label} Range",
                  f"{low_text} - {high_text}", delta=spread_text)
    else:
        st.metric(f"{time_label} Range", "N/A")


def render_market_section(
    latest: pd.DataFrame,
    history: pd.DataFrame,
    time_label: str,
) -> None:
    """Render live price with selected-period context."""
    if latest.empty:
        st.warning("No market data available for this stock.")
        return

    row = latest.iloc[0]
    price = row["close"]

    summary = summarise_history(history)
    comparison_caption, period_change = build_period_caption(
        price, summary["period_start_open"], time_label,
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        _render_price_metric(price, period_change)
    with col2:
        _render_range_metric(
            time_label, summary["period_low"], summary["period_high"])
    with col3:
        st.metric(f"{time_label} Volume",
                  format_volume(summary["period_volume"]))

    st.caption(comparison_caption)

    if summary["trend_msg"]:
        st.info(f"📊 Trend: {summary['trend_msg']}")


# ---------------------------------------------------------------------------
#  News section
# ---------------------------------------------------------------------------
def _news_sentiment_counts(news: pd.DataFrame) -> tuple[int, int, int]:
    """Return (positive, negative, neutral) article counts."""
    positive_count = int(
        (news["sentiment_score"] > SENTIMENT_POSITIVE_THRESHOLD).sum())
    negative_count = int(
        (news["sentiment_score"] < SENTIMENT_NEGATIVE_THRESHOLD).sum())
    neutral_count = len(news) - positive_count - negative_count
    return positive_count, negative_count, neutral_count


def _news_summary_banner(positive_count: int, negative_count: int, neutral_count: int) -> str:
    """Build the one-line news sentiment summary string."""
    prefix = "**What the news is saying:** "
    if positive_count > negative_count:
        return prefix + f"Overall **positive sentiment**. {positive_count} positive vs {negative_count} negative articles."
    if negative_count > positive_count:
        return prefix + f"Overall **negative sentiment**. {negative_count} negative vs {positive_count} positive articles."
    return prefix + f"**Mixed sentiment**. {positive_count} positive, {negative_count} negative, {neutral_count} neutral."


def _render_news_card(row: pd.Series) -> None:
    """Render a single news article card inside a scrollable container."""
    sentiment_score = row["sentiment_score"]
    color = "green" if sentiment_score > ARTICLE_COLOUR_POS_THRESHOLD else "red" if sentiment_score < ARTICLE_COLOUR_NEG_THRESHOLD else "gray"
    confidence = row.get("confidence", "—")

    with st.container(border=True):
        c1, c2 = st.columns([1, 4])
        c1.markdown(f"### :{color}[{sentiment_score:+.1f}]")
        c1.caption(f"Rel: {row['relevance_score']:.2f} | {confidence}")

        article_url = row.get("url", "")
        if article_url:
            title_html = f'<a href="{article_url}" target="_blank" rel="noopener noreferrer"><strong>{row["title"]}</strong></a>'
            c2.markdown(title_html, unsafe_allow_html=True)
        else:
            c2.markdown(f"**{row['title']}**")

        c2.markdown(
            f"*{row['source']} • {row['published_date'].strftime('%d %b')}*")

        if row["analysis"]:
            st.markdown(f"> **AI Take:** {row['analysis']}")


def render_news_section(news: pd.DataFrame) -> None:
    """Render news sentiment metrics, summary banner, and recent article cards."""
    if news.empty:
        st.info("No news articles found for this stock yet.")
        return

    sentiment_avg = news["sentiment_score"].mean()
    positive_count, negative_count, neutral_count = _news_sentiment_counts(
        news)

    last_updated = pd.to_datetime(news["published_date"]).max()
    last_updated_str = last_updated.strftime(
        "%d %b %Y, %H:%M") if pd.notna(last_updated) else "N/A"

    col1, col2, col3 = st.columns(3)
    with col1:
        sentiment_label, _ = classify_sentiment(sentiment_avg)
        st.metric("News Sentiment", sentiment_label, f"{sentiment_avg:.2f}")
    with col2:
        st.metric("Last Updated", last_updated_str)
    with col3:
        st.metric("Articles Tracked", len(news))

    st.info(_news_summary_banner(positive_count, negative_count, neutral_count))

    scroll_container_height = NEWS_CARD_HEIGHT_PX * NEWS_CARDS_VISIBLE

    with st.expander("Recent Coverage", expanded=False):
        with st.container(height=scroll_container_height):
            for _, row in news.iterrows():
                _render_news_card(row)


# ---------------------------------------------------------------------------
#  Social / Reddit section
# ---------------------------------------------------------------------------
def _social_sentiment_summary(positive_count: int, negative_count: int) -> None:
    """Display bullish/bearish summary banner for Reddit sentiment."""
    if positive_count > negative_count:
        summary = f"**Community is bullish.** {positive_count} positive vs {negative_count} negative discussions."
        st.success(summary)
    else:
        summary = f"**Community is bearish.** {negative_count} negative vs {positive_count} positive discussions."
        st.warning(summary)


def _render_discussion_card(row: pd.Series) -> None:
    """Render a single Reddit discussion expander."""
    sentiment_label, _ = classify_sentiment(row["sentiment_score"])
    confidence = row.get("confidence", "Unknown")
    confidence_emoji = CONFIDENCE_EMOJI.get(confidence, "❓")

    with st.expander(f"{sentiment_label} — {row['title'][:60]}... ({row['score']} upvotes)"):
        col_a, col_b = st.columns([2, 1])
        col_a.caption(
            f"Posted: {row['created_at']}, {row['num_comments']} comments")
        col_b.caption(f"{confidence_emoji} Confidence: {confidence}")


def render_social_section(social: pd.DataFrame) -> None:
    """Render Reddit sentiment metrics, engagement stats, and top discussion expanders."""
    if social.empty:
        st.info("No Reddit discussions found for this stock yet.")
        return

    sentiment_avg = social["sentiment_score"].mean()
    positive_count = int(
        (social["sentiment_score"] > SENTIMENT_POSITIVE_THRESHOLD).sum())
    negative_count = int(
        (social["sentiment_score"] < SENTIMENT_NEGATIVE_THRESHOLD).sum())

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        sentiment_label, _ = classify_sentiment(sentiment_avg)
        st.metric("Reddit Sentiment", sentiment_label, f"{sentiment_avg:.2f}")
    with col2:
        st.metric("Total Comments", f"{social['num_comments'].sum():,}")
    with col3:
        st.metric("Top Posts Tracked", len(social))

    _social_sentiment_summary(positive_count, negative_count)

    disc_title, disc_info = st.columns([6, 1])
    with disc_title:
        st.subheader("Top Discussions")
    with disc_info:
        with st.popover("ℹ️"):
            st.markdown(
                "**Top Discussions**\n\n"
                "The most active Reddit posts mentioning this stock, "
                "ranked by engagement.\n\n"
                "- **Sentiment icon** — 🟢 positive, 🟡 neutral, 🔴 negative.\n"
                "- **Upvotes** — community endorsement count.\n"
                "- **Expand** a card to see post date, comment count, "
                "and confidence level."
            )
    for _, row in social.head(TOP_DISCUSSIONS_LIMIT).iterrows():
        _render_discussion_card(row)


# ---------------------------------------------------------------------------
#  Divergence section
# ---------------------------------------------------------------------------
def render_divergence_section(news: pd.DataFrame, social: pd.DataFrame) -> None:
    """Render institutional vs retail sentiment divergence metrics and alert."""
    news_avg = news["sentiment_score"].mean() if not news.empty else 0.0
    social_avg = social["sentiment_score"].mean() if not social.empty else 0.0
    divergence = news_avg - social_avg

    col1, col2, col3 = st.columns(3)
    col1.metric("News", f"{news_avg:+.2f}", delta_color="normal")
    col2.metric("Reddit", f"{social_avg:+.2f}", delta_color="normal")

    div_label = "Aligned" if abs(
        divergence) < DIVERGENCE_ALIGNED_THRESHOLD else "Diverging"
    col3.metric("Source Divergence", div_label,
                delta=f"{divergence:+.2f}", delta_color="normal")

    if abs(divergence) > HIGH_DIVERGENCE_THRESHOLD:
        news_direction = "bullish" if news_avg > 0 else "bearish"
        social_direction = "bullish" if social_avg > 0 else "bearish"
        st.warning(
            f"⚠️ **High Divergence:** Institutions are {news_direction} "
            f"while Retail is {social_direction}. Potential volatility ahead."
        )


# ---------------------------------------------------------------------------
#  Summary Analytics (tabbed chart section)
# ---------------------------------------------------------------------------
def _render_convergence_tab(
    history: pd.DataFrame,
    extended_social: pd.DataFrame,
    is_comparison: bool,
) -> None:
    """Render the Signal Convergence tab content."""
    st.subheader("Price × Reddit Sentiment")
    with st.popover("ℹ️"):
        st.markdown(
            "**Price × Reddit Sentiment**\n\n"
            "Overlays Reddit posts on the stock's price line to show "
            "where community discussion aligns with price movements.\n\n"
            "- **Line** — daily closing price.\n"
            "- **Dots** — Reddit posts placed at the price on their publish date.\n"
            "- **Dot size** — relevance score (how closely the post relates to the stock).\n"
            "- **Dot colour** — sentiment (red = negative, green = positive).\n"
            "- **Click** a dot to see the full post details below the chart."
        )
    st.caption(
        "Dots on the price line represent Reddit posts. Size = relevance, Colour = sentiment (red → green). "
        "When comparison is enabled, price lines are overlaid by ticker."
    )

    convergence_result = build_signal_convergence_chart(
        history, extended_social)

    if convergence_result is None or convergence_result[0] is None:
        st.info("Not enough overlapping price and Reddit data to render this chart.")
        return

    convergence_chart, social_merged = convergence_result
    event = st.altair_chart(
        convergence_chart, use_container_width=True, key="convergence_chart")

    selected_points = event.get(
        "convergence_sel", []) if isinstance(event, dict) else []
    if not selected_points:
        st.caption("Hover over dots to see post details in tooltips.")
        return

    _render_selected_convergence_posts(
        selected_points, social_merged, is_comparison)


def _render_selected_convergence_posts(
    selected_points: list,
    social_merged: pd.DataFrame,
    is_comparison: bool,
) -> None:
    """Display details for posts selected on the convergence chart."""
    selected_post_ids = [point.get("post_id")
                         for point in selected_points if point.get("post_id")]

    if is_comparison:
        selected_tickers = [
            point.get("ticker") for point in selected_points if point.get("ticker")]
        filtered = social_merged[
            social_merged["post_id"].isin(selected_post_ids)
            & social_merged["ticker"].isin(selected_tickers)
        ]
    else:
        filtered = social_merged[social_merged["post_id"].isin(
            selected_post_ids)]

    st.subheader("Selected Post")
    for _, post_row in filtered.iterrows():
        ticker_prefix = f"[{post_row['ticker']}] " if is_comparison else ""
        st.markdown(f"**{ticker_prefix}{post_row['title']}**")
        st.caption(
            f"Sentiment: {round(post_row['sentiment_score'], 3)} | "
            f"Relevance: {round(post_row['relevance_score'], 3)}"
        )
        if post_row["contents"]:
            max_content_length = 600
            st.write(post_row["contents"][:max_content_length])


def _render_momentum_tab(extended_social: pd.DataFrame) -> None:
    """Render the Sentiment Momentum tab content."""
    st.subheader("7-Day Weighted Sentiment Momentum")
    with st.popover("ℹ️"):
        st.markdown(
            "**7-Day Weighted Sentiment Momentum**\n\n"
            "Shows how community sentiment is trending over time using a "
            "7-day rolling average.\n\n"
            "- Each day's sentiment is **weighted by relevance** — higher-relevance "
            "posts count more.\n"
            "- 🟢 **Green area** = net positive sentiment days.\n"
            "- 🔴 **Red area** = net negative sentiment days.\n"
            "- **White line** = rolling average midpoint.\n"
            "- In comparison mode, each ticker gets its own coloured line."
        )
    st.caption(
        "Rolling average of Reddit sentiment weighted by relevance score. "
        "In comparison mode, each line represents one ticker."
    )

    momentum_chart = build_sentiment_momentum_chart(extended_social)
    if momentum_chart is None:
        st.info("Not enough data points to calculate sentiment momentum.")
        return

    st.altair_chart(momentum_chart, use_container_width=True)


def _render_comments_tab(extended_social: pd.DataFrame) -> None:
    """Render the Comments vs Sentiment tab content."""
    st.subheader("Comments vs. Sentiment")
    with st.popover("ℹ️"):
        st.markdown(
            "**Comments vs. Sentiment**\n\n"
            "Scatter plot revealing the relationship between discussion volume "
            "and sentiment direction.\n\n"
            "- **X-axis** — sentiment score (-1 to +1).\n"
            "- **Y-axis** — number of comments on the post.\n"
            "- **Bubble size** — upvote count.\n"
            "- **Colour** — relevance score (viridis gradient). In comparison "
            "mode, colour shows the ticker instead.\n\n"
            "Posts with high comments + negative sentiment often indicate "
            "heated bearish debate."
        )
    st.caption(
        "High comments + negative sentiment can indicate heated bearish debate. "
        "In comparison mode, colour shows ticker and bubble size shows upvotes."
    )

    comments_chart = build_comments_vs_sentiment_chart(extended_social)
    if comments_chart is None:
        st.info("No Reddit data available.")
        return

    st.altair_chart(comments_chart, use_container_width=True)


def _render_indicator_tab(
    news: pd.DataFrame,
    social: pd.DataFrame,
    history: pd.DataFrame,
) -> None:
    """Render the Sources Sentiment Overview tab content."""
    st.subheader("Signal Overview")
    with st.popover("ℹ️"):
        st.markdown(
            "**Sources Sentiment Overview**\n\n"
            "At-a-glance traffic-light indicators for three data sources:\n\n"
            "- **News** — relevance-weighted average sentiment from RSS articles.\n"
            "- **Reddit** — relevance-weighted average sentiment from Reddit posts.\n"
            "- **Market** — price change direction (positive / negative / neutral) "
            "over the selected period.\n\n"
            "🟢 Positive · 🟡 Neutral · 🔴 Negative\n\n"
            "In comparison mode, each row represents a different ticker."
        )
    st.caption(
        "Aggregate sentiment for each source at a glance. "
        "In comparison mode, each row is a different ticker."
    )

    indicator_chart = build_sentiment_indicator_row(news, social, history)
    st.altair_chart(indicator_chart, use_container_width=True)


def render_summary_analytics(
    history: pd.DataFrame,
    extended_social: pd.DataFrame,
    social: pd.DataFrame,
    news: pd.DataFrame,
) -> None:
    """Render the Summary Analytics tab group with all interactive Altair charts."""
    analytics_title, analytics_info = st.columns([6, 1])
    with analytics_title:
        st.header("Summary Analytics")
    with analytics_info:
        with st.popover("ℹ️"):
            st.markdown(
                "**Summary Analytics**\n\n"
                "Four interactive chart tabs exploring the relationship between "
                "price, news, and Reddit sentiment:\n\n"
                "1. **Signal Convergence** — price line overlaid with Reddit sentiment dots.\n"
                "2. **Sentiment Momentum** — 7-day rolling weighted sentiment.\n"
                "3. **Comments vs Sentiment** — scatter of discussion volume vs sentiment.\n"
                "4. **Sources Overview** — traffic-light indicators for News, Reddit, and Market.\n\n"
                "When a comparison ticker is added, all charts update to show both tickers."
            )
    st.caption(
        "Interactive charts. Hover for tooltips. Click sentiment dots in Chart 1 to inspect posts.")

    is_comparison = "ticker" in history.columns and history["ticker"].nunique(
    ) > 1
    if is_comparison:
        compared_tickers = ", ".join(
            sorted(history["ticker"].dropna().unique().tolist()))
        st.caption(f"Comparison mode: {compared_tickers}")

    va_tab1, va_tab2, va_tab3, va_tab4 = st.tabs([
        "📌 Signal Convergence",
        "📈 Sentiment Momentum",
        "💬 Comments vs Sentiment",
        "💭 Sources Sentiment Overview",
    ])

    with va_tab1:
        _render_convergence_tab(history, extended_social, is_comparison)
    with va_tab2:
        _render_momentum_tab(extended_social)
    with va_tab3:
        _render_comments_tab(extended_social)
    with va_tab4:
        _render_indicator_tab(news, social, history)


# ---------------------------------------------------------------------------
#  RAG company summary
# ---------------------------------------------------------------------------
def get_company_summary(ticker: str, company_name: str) -> str:
    """Calls RAG to get plain english summary of a specific stock."""
    payload = {
        "question": f"Generate a summary for {company_name} ({ticker}) including recent price context, news, and sentiment.",
        "ticker": ticker,
    }
    try:
        response = requests.post(RAG_API_URL, json=payload, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        return "⚠️ Company summary is temporarily unavailable. Please try again later."
    return response.json().get("answer", "No summary returned.")
