import os
import psycopg2
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    """Establish connection to PostgreSQL RDS database."""
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", "5432")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME"),
            sslmode="require"
        )
    except psycopg2.DatabaseError as err:
        st.error("Failed to connect to database. Check environment variables.")
        raise


def get_stock_by_ticker_or_name(search_term):
    """Search for stock by ticker or name."""
    conn = get_db_connection()
    cursor = conn.cursor()
    search_lower = search_term.lower()

    cursor.execute("""
        SELECT stock_id, ticker, stock_name FROM stock
        WHERE LOWER(ticker) = %s OR LOWER(stock_name) LIKE %s
        LIMIT 1
    """, (search_lower, f"%{search_lower}%"))

    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result


def get_market_data(stock_id):
    """Get latest market data and historical trend."""
    conn = get_db_connection()

    latest = pd.read_sql_query("""
        SELECT close, open, high, low, volume, latest_time FROM alpaca_live
        WHERE stock_id = %s
        ORDER BY latest_time DESC LIMIT 1
    """, conn, params=(stock_id,))

    history = pd.read_sql_query("""
        SELECT bar_date, open, high, low, close, volume FROM alpaca_history
        WHERE stock_id = %s
        ORDER BY bar_date DESC LIMIT 30
    """, conn, params=(stock_id,))

    conn.close()
    return latest, history


def get_news_signals(stock_id):
    """Get RSS news articles with sentiment analysis."""
    conn = get_db_connection()
    news = pd.read_sql_query("""
        SELECT ra.sentiment_score, ra.relevance_score, ra.analysis,
               rss.title, rss.summary, rss.published_date, rss.source
        FROM rss_analysis ra
        JOIN rss_article rss ON ra.story_id = rss.story_id
        WHERE ra.stock_id = %s
        ORDER BY rss.published_date DESC LIMIT 20
    """, conn, params=(stock_id,))
    conn.close()
    return news


def get_social_signals(stock_id):
    """Get Reddit posts with sentiment analysis."""
    conn = get_db_connection()
    social = pd.read_sql_query("""
        SELECT ra.sentiment_score, ra.relevance_score, ra.analysis,
               rp.title, rp.score, rp.num_comments, rp.created_at, rp.url
        FROM reddit_analysis ra
        JOIN reddit_post rp ON ra.story_id = rp.post_id
        WHERE ra.stock_id = %s
        ORDER BY rp.created_at DESC LIMIT 20
    """, conn, params=(stock_id,))
    conn.close()
    return social


def classify_sentiment(score):
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
                          f"{format_price(low)} - {format_price(high)}")
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
            for idx, row in news.head(5).iterrows():
                sentiment_label, _ = classify_sentiment(row["sentiment_score"])
                with st.expander(
                    f"{sentiment_label} — {row['title'][:70]}... ({row['source']})"
                ):
                    st.caption(f"Published: {row['published_date']}")
                    st.write(row["summary"][:300] + "...")
                    if row["analysis"]:
                        st.markdown(
                            f"**Analysis:** {row['analysis'][:200]}...")

        st.divider()

        # --- SOCIAL SIGNALS SECTION ---
        st.header("Community Sentiment")

        social = get_social_signals(stock_id)

        if social.empty:
            st.info("No Reddit discussions found for this stock yet.")
        else:
            sentiment_avg = social["sentiment_score"].mean()

            col1, col2, col3 = st.columns(3)
            with col1:
                sentiment_label, _ = classify_sentiment(sentiment_avg)
                st.metric("Reddit Sentiment", sentiment_label,
                          f"{sentiment_avg:.2f}")
            with col2:
                engagement = social["num_comments"].sum()
                st.metric("Total Comments", f"{engagement:,}")
            with col3:
                st.metric("Top Posts Tracked", len(social))

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
                with st.expander(
                    f"{sentiment_label} — {row['title'][:60]}... ({row['score']} upvotes)"
                ):
                    st.caption(
                        f"Posted: {row['created_at']}, {row['num_comments']} comments")
                    if row["analysis"]:
                        st.markdown(
                            f"**Community Take:** {row['analysis'][:200]}...")

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
