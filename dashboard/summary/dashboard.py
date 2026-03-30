import altair as alt
import streamlit as st
import pandas as pd
from queries import (
    get_stock_by_ticker_or_name,
    get_market_data,
    get_news_signals,
    get_social_signals,
    get_extended_social,
)


def build_signal_convergence_chart(history: pd.DataFrame, social: pd.DataFrame) -> alt.LayerChart | None:
    """Layer price line with Reddit sentiment dots. Click a dot to surface the post below."""
    if history.empty or social.empty:
        return None

    history = history.copy()
    history["bar_date"] = pd.to_datetime(history["bar_date"])

    social = social.copy()
    social["date"] = pd.to_datetime(social["created_at"]).dt.normalize()
    social_merged = social.merge(
        history[["bar_date", "close"]], left_on="date", right_on="bar_date", how="inner"
    )

    if social_merged.empty:
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
    """Rolling 7-day relevance-weighted sentiment area chart, green above zero and red below."""
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
    """Scatter of upvotes vs sentiment, colored by relevance. High-neg + high-ups = retail panic signal."""
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
            ],
        )
    )

    return (zero_rule + scatter).properties(height=300)


def build_news_horizon_chart(news: pd.DataFrame) -> alt.Chart | None:
    """Strip plot of news coverage density by source over time. Clusters signal breaking news."""
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
            ],
        )
        .properties(height=chart_height)
    )


def classify_sentiment(score: float | None) -> tuple[str, str]:
    """Convert sentiment score to plain English classification."""
    if score is None:
        return "Unknown", "gray"
    if score > 0.5:
        return "🟢 Positive", "green"
    elif score < -0.5:
        return "🔴 Negative", "red"
    else:
        return "🟡 Neutral", "orange"


def format_price(val):
    """Format price with 2 decimals."""
    return f"${val:,.2f}" if val else "N/A"


def dashboard():
    st.set_page_config(
        page_title="Stock Intelligence Dashboard", layout="wide")
    st.title("📊 Stock Intelligence Dashboard")
    st.caption(
        "Consolidated view of market data, news signals, and community sentiment")
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

                with st.container(border=True):
                    c1, c2 = st.columns([1, 4])
                    c1.markdown(f"### :{color}[{s_score:+.1f}]")
                    c1.caption(f"Rel: {row['relevance_score']:.2f}")
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

            # Sentiment drivers
            st.subheader("Sentiment Drivers")
            top_keywords = social["title"].str.split(
                expand=True).stack().value_counts().head(10)
            st.write("Top Keywords:")
            st.write(top_keywords)

            # Recent posts
            st.subheader("Top Discussions")
            for idx, row in social.head(5).iterrows():
                sentiment_label, _ = classify_sentiment(row["sentiment_score"])
                with st.expander(
                    f"{sentiment_label} — {row['title'][:60]}... ({row['score']} upvotes)"
                ):
                    st.caption(
                        f"Posted: {row['created_at']}, {row['num_comments']} comments")
                    if row["analysis"]:
                        st.markdown(
                            f"**Community Take:** {row['analysis'][:200]}...")

            # Enhanced visualization
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

            st.subheader("Sentiment Heatmap")
            heatmap = (
                alt.Chart(social)
                .mark_rect()
                .encode(
                    x=alt.X("created_at:T", title="Date"),
                    y=alt.Y("sentiment_score:Q", title="Sentiment Score"),
                    color=alt.Color("relevance_score:Q", scale=alt.Scale(
                        scheme="viridis"), title="Relevance"),
                    tooltip=["title:N", "sentiment_score:Q",
                             "relevance_score:Q"]
                )
            )
            st.altair_chart(heatmap, use_container_width=True)

            st.caption(
                "Heatmap shows sentiment over time with relevance as intensity.")

        st.divider()

        # --- SIGNAL CONVERGENCE HEADER (PULSE CHECK) ---
        st.subheader(f"Pulse Check: {ticker}")
        st.caption("Institutional vs. Retail Sentiment Alignment")

        # news = get_news_signals(stock_id)
        # social = get_social_signals(stock_id)

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

        va_tab1, va_tab2, va_tab3, va_tab4 = st.tabs([
            "📌 Signal Convergence",
            "📈 Sentiment Momentum",
            "💥 Engagement Matrix",
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
