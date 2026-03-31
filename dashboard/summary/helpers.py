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

RAG_API_URL = os.environ["RAG_API_URL"]


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

    trend_msg = None
    if len(history) >= 5:
        recent_closes = history["close"].tail(5).tolist()
        trend_msg = (
            "**Uptrend.** Recent prices are rising."
            if recent_closes[-1] > recent_closes[0]
            else "**Downtrend.** Recent prices are falling."
            if recent_closes[-1] < recent_closes[0]
            else "**Sideways.** Prices are stable."
        )

    return {
        "period_low": history["low"].min(),
        "period_high": history["high"].max(),
        "period_volume": history["volume"].sum(),
        "period_start_open": history.iloc[0]["open"],
        "trend_msg": trend_msg,
    }


def build_period_caption(price: float, period_start_open: float | None, time_label: str) -> tuple[str, float | None]:
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


def render_market_section(latest: pd.DataFrame, history: pd.DataFrame, time_label: str):
    """Render live price with selected-period context."""
    if latest.empty:
        st.warning("No market data available for this stock.")
        return

    row = latest.iloc[0]
    price = row["close"]
    latest_time = row.get("latest_time")

    summary = summarise_history(history)

    comparison_caption, period_change = build_period_caption(
        price,
        summary["period_start_open"],
        time_label,)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Current Price",
            format_price(price),
            f"{period_change:+.2f}" if period_change is not None else None,
        )

    with col2:
        if summary["period_low"] is not None and summary["period_high"] is not None:
            low_text = format_price(summary["period_low"]).replace("$", r"\$")
            high_text = format_price(
                summary["period_high"]).replace("$", r"\$")
            spread_text = format_price(
                summary["period_high"] - summary["period_low"]
            ).replace("$", r"\$")

            st.metric(
                f"{time_label} Range",
                f"{low_text} - {high_text}",
                delta=spread_text,
            )
        else:
            st.metric(f"{time_label} Range", "N/A")

    with col3:
        st.metric(f"{time_label} Volume",
                  format_volume(summary["period_volume"]))

    st.caption(comparison_caption)

    if summary["trend_msg"]:
        st.info(f"📊 Trend: {summary['trend_msg']}")


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


def get_company_summary(ticker: str, company_name: str) -> str:
    """Calls RAG to get plain english summary of a specific stock"""
    payload = {
        "question": f"Generate a summary for {company_name} ({ticker}) including recent price context, news, and sentiment.",
        "ticker": ticker
    }

    response = requests.post(RAG_API_URL, json=payload, timeout=30)
    return response.json().get("answer", "No summary returned.")
