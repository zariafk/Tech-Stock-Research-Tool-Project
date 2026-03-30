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
    build_news_horizon_chart,
    build_comments_vs_sentiment_chart,
)

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


def classify_sentiment(score: float | None) -> tuple[str, str]:
    """Converts a -1 to +1 sentiment score into a label and colour string."""
    if score is None:
        return "Unknown", "gray"
    if score > 0.5:
        return "🟢 Positive", "green"
    if score < -0.5:
        return "🔴 Negative", "red"
    return "🟡 Neutral", "orange"


def format_price(val: float | None) -> str:
    """Formats a numeric price as a USD string with 2 decimal places."""
    return f"${val:,.2f}" if val else "N/A"


def render_market_section(latest: pd.DataFrame, history: pd.DataFrame):
    """Render live price metrics and short-term trend context."""
    if latest.empty:
        st.warning("No market data available for this stock.")
        return

    row = latest.iloc[0]
    price = row["close"]
    open_price = row["open"]
    change = price - open_price
    change_pct = (change / open_price * 100) if open_price else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Current Price", format_price(price), f"{change:+.2f}")
    with col2:
        st.metric(
            "Today's Range",
            f"{format_price(row['low'])} - {format_price(row['high'])}",
            delta=format_price(row["high"] - row["low"]),
        )
    with col3:
        volume = row["volume"]
        st.metric("Volume", f"{volume / 1e3:.1f}K" if volume else "N/A")

    direction = "up" if change >= 0 else "down"
    st.caption(f"📈 Price {direction} {abs(change_pct):.2f}% from open")

    min_history_rows = 5
    if history.shape[0] >= min_history_rows:
        recent_closes = history["close"].head(min_history_rows).tolist()
        if recent_closes[0] > recent_closes[-1]:
            trend_msg = "**Uptrend.** Recent prices are rising."
        elif recent_closes[0] < recent_closes[-1]:
            trend_msg = "**Downtrend.** Recent prices are falling."
        else:
            trend_msg = "**Sideways.** Prices are stable."
        st.info(f"📊 Trend: {trend_msg}")


def render_news_section(news: pd.DataFrame):
    """Render news sentiment metrics, summary banner, and recent article cards."""
    if news.empty:
        st.info("No news articles found for this stock yet.")
        return

    sentiment_avg = news["sentiment_score"].mean()
    relevance_avg = news["relevance_score"].mean()
    positive_count = (news["sentiment_score"] > 0.5).sum()
    negative_count = (news["sentiment_score"] < -0.5).sum()
    neutral_count = len(news) - positive_count - negative_count

    col1, col2, col3 = st.columns(3)
    with col1:
        sentiment_label, _ = classify_sentiment(sentiment_avg)
        st.metric("News Sentiment", sentiment_label, f"{sentiment_avg:.2f}")
    with col2:
        st.metric("Average Relevance", f"{relevance_avg:.2f}")
    with col3:
        st.metric("Articles Tracked", len(news))

    summary = "**What the news is saying:** "
    if positive_count > negative_count:
        summary += f"Overall **positive sentiment**. {positive_count} positive vs {negative_count} negative articles."
    elif negative_count > positive_count:
        summary += f"Overall **negative sentiment**. {negative_count} negative vs {positive_count} positive articles."
    else:
        summary += f"**Mixed sentiment**. {positive_count} positive, {negative_count} negative, {neutral_count} neutral."
    st.info(summary)

    st.subheader("Recent Coverage")
    for _, row in news.head(5).iterrows():
        s_score = row["sentiment_score"]
        color = "green" if s_score > 0.2 else "red" if s_score < -0.2 else "gray"
        confidence = row.get("confidence", "—")
        with st.container(border=True):
            c1, c2 = st.columns([1, 4])
            c1.markdown(f"### :{color}[{s_score:+.1f}]")
            c1.caption(f"Rel: {row['relevance_score']:.2f} | {confidence}")
            c2.markdown(f"**{row['title']}**")
            c2.markdown(
                f"*{row['source']} • {row['published_date'].strftime('%d %b')}*")
            if row["analysis"]:
                st.markdown(f"> **AI Take:** {row['analysis']}")


def render_social_section(social: pd.DataFrame):
    """Render Reddit sentiment metrics, engagement stats, and top discussion expanders."""
    if social.empty:
        st.info("No Reddit discussions found for this stock yet.")
        return

    sentiment_avg = social["sentiment_score"].mean()
    engagement_velocity = social["created_at"].diff().dt.total_seconds().mean()
    positive_count = (social["sentiment_score"] > 0.5).sum()
    negative_count = (social["sentiment_score"] < -0.5).sum()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        sentiment_label, _ = classify_sentiment(sentiment_avg)
        st.metric("Reddit Sentiment", sentiment_label, f"{sentiment_avg:.2f}")
    with col2:
        st.metric("Total Comments", f"{social['num_comments'].sum():,}")
    with col3:
        st.metric("Top Posts Tracked", len(social))

    if positive_count > negative_count:
        summary = f"**Community is bullish.** {positive_count} positive vs {negative_count} negative discussions."
        st.success(summary)
    else:
        summary = f"**Community is bearish.** {negative_count} negative vs {positive_count} positive discussions."
        st.warning(summary)

    st.subheader("Top Discussions")
    for _, row in social.head(5).iterrows():
        sentiment_label, _ = classify_sentiment(row["sentiment_score"])
        confidence = row.get("confidence", "Unknown")
        confidence_emoji = CONFIDENCE_EMOJI.get(confidence, "❓")
        with st.expander(f"{sentiment_label} — {row['title'][:60]}... ({row['score']} upvotes)"):
            col_a, col_b = st.columns([2, 1])
            col_a.caption(
                f"Posted: {row['created_at']}, {row['num_comments']} comments")
            col_b.caption(f"{confidence_emoji} Confidence: {confidence}")


def render_divergence_section(news: pd.DataFrame, social: pd.DataFrame):
    """Render institutional vs retail sentiment divergence metrics and alert."""
    news_avg = news["sentiment_score"].mean() if not news.empty else 0.0
    social_avg = social["sentiment_score"].mean() if not social.empty else 0.0
    divergence = news_avg - social_avg

    col1, col2, col3 = st.columns(3)
    col1.metric("News", f"{news_avg:+.2f}", delta_color="normal")
    col2.metric("Reddit", f"{social_avg:+.2f}", delta_color="normal")
    div_label = "Aligned" if abs(divergence) < 0.3 else "Diverging"
    col3.metric("Source Divergence", div_label,
                delta=f"{divergence:+.2f}", delta_color="normal")

    high_divergence_threshold = 0.5
    if abs(divergence) > high_divergence_threshold:
        news_direction = "bullish" if news_avg > 0 else "bearish"
        social_direction = "bullish" if social_avg > 0 else "bearish"
        st.warning(
            f"⚠️ **High Divergence:** Institutions are {news_direction} while Retail is {social_direction}. Potential volatility ahead."
        )


def render_visual_analytics(history: pd.DataFrame, extended_social: pd.DataFrame, social: pd.DataFrame, news: pd.DataFrame):
    """Render the Visual Analytics tab group with all interactive Altair charts."""
    st.header("Visual Analytics")
    st.caption(
        "Interactive charts. Hover for tooltips. Click sentiment dots in Chart 1 to inspect posts.")

    va_tab1, va_tab2, va_tab3, va_tab4, va_tab5 = st.tabs([
        "📌 Signal Convergence",
        "📈 Sentiment Momentum",
        "📊 Engagement Matrix",
        "💬 Comments vs Sentiment",
        "📰 News Horizon",
    ])

    with va_tab1:
        st.subheader("Price × Reddit Sentiment")
        st.caption("Dots on the price line represent Reddit posts. Size = relevance, Colour = sentiment (red → green). Click a dot to inspect the post.")
        convergence_result = build_signal_convergence_chart(
            history, extended_social)
        if convergence_result is None or convergence_result[0] is None:
            st.info(
                "Not enough overlapping price and Reddit data to render this chart.")
        else:
            convergence_chart, social_merged = convergence_result
            event = st.altair_chart(
                convergence_chart, use_container_width=True, key="convergence_chart")
            selected_points = event.get(
                "convergence_sel", []) if isinstance(event, dict) else []
            if selected_points:
                selected_ids = [p.get("post_id")
                                for p in selected_points if p.get("post_id")]
                filtered = social_merged[social_merged["post_id"].isin(
                    selected_ids)]
                st.subheader("Selected Post")
                for _, post_row in filtered.iterrows():
                    st.markdown(f"**{post_row['title']}**")
                    st.caption(
                        f"Sentiment: {round(post_row['sentiment_score'], 3)} | Relevance: {round(post_row['relevance_score'], 3)}")
                    if post_row["contents"]:
                        st.write(post_row["contents"][:600])
            else:
                st.caption("Hover over dots to see post details in tooltips.")

    with va_tab2:
        st.subheader("7-Day Weighted Sentiment Momentum")
        st.caption(
            "Rolling average of Reddit sentiment weighted by relevance score. Green = bullish momentum, Red = bearish.")
        momentum_chart = build_sentiment_momentum_chart(extended_social)
        if momentum_chart is None:
            st.info("Not enough data points to calculate sentiment momentum.")
        else:
            st.altair_chart(momentum_chart, use_container_width=True)

    with va_tab3:
        st.subheader("Engagement vs. Sentiment")
        st.caption(
            "Each point is a Reddit post. High upvotes + negative sentiment = potential retail panic signal.")
        scatter_chart = build_engagement_scatter_chart(social)
        if scatter_chart is None:
            st.info("No Reddit engagement data available.")
        else:
            st.altair_chart(scatter_chart, use_container_width=True)
            st.caption("Quadrant guide: top-right = popular & bullish | bottom-left = ignored & bearish | **top-left = high engagement & negative = watch carefully**")

    with va_tab4:
        st.subheader("Comments vs. Sentiment")
        st.caption(
            "High comments + negative sentiment (top-left) = heated bearish debate. Bubble size = upvotes.")
        comments_chart = build_comments_vs_sentiment_chart(extended_social)
        if comments_chart is None:
            st.info("No Reddit data available.")
        else:
            st.altair_chart(comments_chart, use_container_width=True)

    with va_tab5:
        st.subheader("News Coverage Density")
        st.caption(
            "Each circle is an article. Vertical clusters mean multiple outlets published simultaneously — a likely news event.")
        horizon_chart = build_news_horizon_chart(news)
        if horizon_chart is None:
            st.info("No news data available to render this chart.")
        else:
            st.altair_chart(horizon_chart, use_container_width=True)
