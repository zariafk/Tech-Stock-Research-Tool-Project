import altair as alt
import streamlit as st
import pandas as pd
from .queries import (
    get_stock_by_ticker_or_name,
    get_market_data,
    get_news_signals,
    get_social_signals,
    get_extended_social,
)
from .charts import (
    build_comments_vs_sentiment_chart,
    build_signal_convergence_chart,
    build_sentiment_momentum_chart,
    build_engagement_scatter_chart,
    build_news_horizon_chart,
    classify_sentiment,
    format_price,
)


def dashboard():
    st.caption(
        "Consolidated view of market data, news signals, and community sentiment for specific stocks.")
    st.divider()

    col1, col2 = st.columns([3, 1])
    with col1:
        search_input = st.text_input(
            "Search by ticker or company name",
            placeholder="e.g., AAPL or Apple",
            key="stock_search"
        )
    with col2:
        st.write("")  # spacer
        search_btn = st.button("Search", use_container_width=True)

    if search_btn or search_input:
        if not search_input:
            st.warning("Please enter a stock ticker or company name.")
            return
        stock_result = get_stock_by_ticker_or_name(search_input)

        if not stock_result:
            st.error("Stock not found. Please check the ticker or company name.")
            return

        stock_id, ticker, company_name = stock_result
        st.divider()

        extended_social = get_extended_social(stock_id)

        # --- MARKET DATA SECTION ---
        st.header(f"Market Data — {ticker} ({company_name})")

        latest, history = get_market_data(stock_id)

        if latest.empty:
            st.warning("No market data available for this stock.")
        else:
            row = latest.iloc[0]
            price = row["close"]
            open_price = row["open"]
            high = row["high"]
            low = row["low"]
            volume = row["volume"]
            timestamp = row["latest_time"]

            change = price - open_price
            change_pct = (change / open_price * 100) if open_price else 0

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Current Price", format_price(
                    price), f"{change:+.2f}")
            with col2:
                st.metric("Today's Range",
                          f"{format_price(low)} - {format_price(high)}",
                          delta=f"{format_price(high - low)}")
            with col3:
                st.metric("Volume", f"{volume/1e6:.1f}M" if volume else "N/A")
            with col4:
                st.metric("As of", timestamp.strftime(
                    "%I:%M %p") if timestamp else "N/A")

            st.caption(
                f"📈 Price {'up' if change >= 0 else 'down'} {abs(change_pct):.2f}% from open")

            # Market context
            if history.shape[0] >= 5:
                recent_closes = history["close"].head(5).tolist()
                trend_msg = ""
                if recent_closes[0] > recent_closes[-1]:
                    trend_msg = "**Uptrend.** Recent prices are rising."
                elif recent_closes[0] < recent_closes[-1]:
                    trend_msg = "**Downtrend.** Recent prices are falling."
                else:
                    trend_msg = "**Sideways.** Prices are stable."
                st.info(f"📊 Trend: {trend_msg}")

        st.divider()

        # --- NEWS SIGNALS SECTION ---
        st.header(f"News & Market Signals")

        news = get_news_signals(stock_id)

        if news.empty:
            st.info("No news articles found for this stock yet.")
        else:
            sentiment_avg = news["sentiment_score"].mean()
            relevance_avg = news["relevance_score"].mean()

            col1, col2, col3 = st.columns(3)
            with col1:
                sentiment_label, _ = classify_sentiment(sentiment_avg)
                st.metric("News Sentiment", sentiment_label,
                          f"{sentiment_avg:.2f}")
            with col2:
                st.metric("Average Relevance", f"{relevance_avg:.2f}")
            with col3:
                st.metric("Articles Tracked", len(news))

            # News summary
            positive_count = (news["sentiment_score"] > 0.5).sum()
            negative_count = (news["sentiment_score"] < -0.5).sum()
            neutral_count = len(news) - positive_count - negative_count

            summary_text = f"**What the news is saying:** "
            if positive_count > negative_count:
                summary_text += f"Overall **positive sentiment**. {positive_count} positive vs {negative_count} negative articles."
            elif negative_count > positive_count:
                summary_text += f"Overall **negative sentiment**. {negative_count} negative vs {positive_count} positive articles."
            else:
                summary_text += f"**Mixed sentiment**. {positive_count} positive, {negative_count} negative, {neutral_count} neutral."

            st.info(summary_text)

            # Recent articles
            st.subheader("Recent Coverage")
            for _, row in news.head(5).iterrows():
                s_score = row["sentiment_score"]
                color = "green" if s_score > 0.2 else "red" if s_score < -0.2 else "gray"
                confidence = row.get("confidence", "Unknown")
                confidence_emoji = {"High": "✅", "Medium": "⚠️",
                                    "Low": "❌", "Unknown": "❓"}.get(confidence, "❓")

                with st.container(border=True):
                    c1, c2 = st.columns([1, 4])
                    c1.markdown(f"### :{color}[{s_score:+.1f}]")
                    confidence = row.get("confidence", "—")
                    c1.caption(
                        f"Rel: {row['relevance_score']:.2f} | {confidence}")

                    c2.markdown(f"**{row['title']}**")
                    c2.markdown(
                        f"*{row['source']} • {row['published_date'].strftime('%d %b')}*"
                    )
                    if row["analysis"]:
                        st.markdown(f"> **AI Take:** {row['analysis']}")

        st.divider()

        # --- SOCIAL SIGNALS SECTION ---
        st.header("Community Sentiment")

        social = get_social_signals(stock_id)

        if social.empty:
            st.info("No Reddit discussions found for this stock yet.")
        else:
            sentiment_avg = social["sentiment_score"].mean()
            engagement_velocity = social["created_at"].diff(
            ).dt.total_seconds().mean()

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                sentiment_label, _ = classify_sentiment(sentiment_avg)
                st.metric("Reddit Sentiment", sentiment_label,
                          f"{sentiment_avg:.2f}")
            with col2:
                engagement = social["num_comments"].sum()
                st.metric("Total Comments", f"{engagement:,}")
            with col3:
                st.metric("Top Posts Tracked", len(social))
            with col4:
                st.metric("Engagement Velocity",
                          f"{engagement_velocity:.2f} sec/post")

            # Social summary
            positive_count = (social["sentiment_score"] > 0.5).sum()
            negative_count = (social["sentiment_score"] < -0.5).sum()

            if len(social) > 0:
                if positive_count > negative_count:
                    summary = f"**Community is bullish.** {positive_count} positive vs {negative_count} negative discussions."
                else:
                    summary = f"**Community is bearish.** {negative_count} negative vs {positive_count} positive discussions."
                st.success(summary) if positive_count > negative_count else st.warning(
                    summary)

            # Recent posts
            st.subheader("Top Discussions")
            for idx, row in social.head(5).iterrows():
                sentiment_label, _ = classify_sentiment(row["sentiment_score"])
                confidence = row.get("confidence", "Unknown")
                confidence_emoji = {"High": "✅", "Medium": "⚠️",
                                    "Low": "❌", "Unknown": "❓"}.get(confidence, "❓")
                with st.expander(
                    f"{sentiment_label} — {row['title'][:60]}... ({row['score']} upvotes)"
                ):
                    col_a, col_b = st.columns([2, 1])
                    with col_a:
                        st.caption(
                            f"Posted: {row['created_at']}, {row['num_comments']} comments")
                    with col_b:
                        st.caption(
                            f"{confidence_emoji} Confidence: {confidence}")

        news_avg = news["sentiment_score"].mean() if not news.empty else 0
        social_avg = social["sentiment_score"].mean(
        ) if not social.empty else 0
        divergence = news_avg - social_avg

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("News",
                      f"{news_avg:+.2f}", delta_color="normal")
        with col2:
            st.metric("Reddit",
                      f"{social_avg:+.2f}", delta_color="normal")
        with col3:
            div_label = "Aligned" if abs(divergence) < 0.3 else "Diverging"
            st.metric("Source Divergence", div_label,
                      delta=f"{divergence:+.2f}", delta_color="normal")

        if abs(divergence) > 0.5:
            bullish_bearish_news = "bullish" if news_avg > 0 else "bearish"
            bullish_bearish_social = "bullish" if social_avg > 0 else "bearish"
            st.warning(
                f"⚠️ **High Divergence:** Institutions are {bullish_bearish_news} while Retail is {bullish_bearish_social}. Potential volatility ahead.")

        st.divider()

        # --- VISUAL ANALYTICS SECTION ---
        st.header("Visual Analytics")
        st.caption(
            "Interactive charts. Hover for tooltips. Click sentiment dots in Chart 1 to inspect posts.")

        va_tab1, va_tab2, va_tab3, va_tab4, va_tab5, va_tab6 = st.tabs([
            "📌 Signal Convergence",
            "📈 Sentiment Momentum",
            "📊 Signal vs Price",
            "💥 Engagement Matrix",
            "💬 Comments vs Sentiment",
            "📰 News Horizon",
        ])

        with va_tab1:
            st.subheader("Price × Reddit Sentiment")
            st.caption(
                "Dots on the price line represent Reddit posts. Size = relevance, Colour = sentiment (red → green). Click a dot to inspect the post.")
            convergence_result = build_signal_convergence_chart(
                history, extended_social)
            if convergence_result is None or convergence_result[0] is None:
                st.info(
                    "Not enough overlapping price and Reddit data to render this chart.")
            else:
                convergence_chart, social_merged = convergence_result
                event = st.altair_chart(
                    convergence_chart, use_container_width=True, key="convergence_chart")
                if isinstance(event, dict) and "convergence_sel" in event:
                    selected_points = event.get("convergence_sel", [])
                else:
                    selected_points = []
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
                    st.caption(
                        "Hover over dots to see post details in tooltips.")

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
                "Each point is a Reddit post. High upvotes + negative sentiment (bottom-left) = potential retail panic signal.")
            scatter_chart = build_engagement_scatter_chart(social)
            if scatter_chart is None:
                st.info("No Reddit engagement data available.")
            else:
                st.altair_chart(scatter_chart, use_container_width=True)
                st.caption(
                    "Quadrant guide: top-right = popular & bullish | bottom-left = ignored & bearish | **top-left = high engagement & negative = watch carefully**")

        with va_tab4:
            st.subheader("News Coverage Density")
            st.caption(
                "Each circle is an article. Vertical clusters mean multiple outlets published simultaneously — a likely news event.")
            horizon_chart = build_news_horizon_chart(news)
            if horizon_chart is None:
                st.info("No news data available to render this chart.")
            else:
                st.altair_chart(horizon_chart, use_container_width=True)

        with va_tab5:
            st.subheader("Comments vs. Sentiment")
            st.caption(
                "High comments + negative sentiment (top-left) = heated bearish debate. Bubble size = upvotes.")
            comments_chart = build_comments_vs_sentiment_chart(extended_social)
            if comments_chart is None:
                st.info("No Reddit data available.")
            else:
                st.altair_chart(comments_chart, use_container_width=True)

        st.divider()

        # --- ECONOMIC CONTEXT ---
        st.header("Broader Economic Context")

        context_msg = f"""
        **{ticker} in context:**

        - **Sector:** This company operates in technology, a sector facing macroeconomic pressures including interest rates, inflation, and AI competition.
        - **What we observe:** The dashboard above shows real-time signals from markets, news outlets, and community discussions about {ticker}.
        - **Interpretation:** Compare the market price movements, news sentiment, and community sentiment. Alignment suggests confidence, divergence suggests uncertainty.
        - **Data Sources:** Market data via Alpaca • News via RSS feeds • Community via Reddit • Analysis via AI sentiment scoring.
        """
        st.info(context_msg)

        st.caption(
            "_Dashboard updated with live data from RDS. Refresh to see latest signals._")


if __name__ == "__main__":
    dashboard()
