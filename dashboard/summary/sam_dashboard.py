"""Signal vs Price dual-axis time series chart."""

import altair as alt
import pandas as pd
import yfinance as yf


def get_price_data(
    ticker: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """Fetches daily closing prices for a ticker."""
    df = yf.download(ticker, start=start, end=end, progress=False)
    df = df[["Close"]].reset_index()
    df.columns = ["date", "close"]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def build_daily_sentiment(
    reddit_analysis: pd.DataFrame,
    reddit_posts: pd.DataFrame,
    stocks: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """Aggregates sentiment to a daily level for a single ticker."""
    analysis = reddit_analysis.merge(stocks, on="stock_id", how="left")
    analysis = analysis[analysis["ticker"] == ticker]

    analysis = analysis.merge(
        reddit_posts[["post_id", "created_at", "ups"]],
        left_on="story_id",
        right_on="post_id",
        how="left",
    )

    analysis["date"] = pd.to_datetime(analysis["created_at"]).dt.date

    # Weighted average: high-upvote posts count more
    daily = (
        analysis.groupby("date")
        .apply(
            lambda g: pd.Series({
                "avg_sentiment": (
                    (g["sentiment_score"] * g["ups"]).sum() / g["ups"].sum()
                    if g["ups"].sum() > 0
                    else g["sentiment_score"].mean()
                ),
                "post_count": len(g),
            })
        )
        .reset_index()
    )

    return daily


def create_signal_vs_price_chart(
    reddit_analysis: pd.DataFrame,
    reddit_posts: pd.DataFrame,
    stocks: pd.DataFrame,
    ticker: str,
    start_date: str,
    end_date: str,
) -> alt.LayerChart:
    """Creates a dual-axis chart comparing daily sentiment to stock price.

    Args:
        reddit_analysis: story_id, stock_id, sentiment_score, relevance_score.
        reddit_posts: post_id, created_at, ups.
        stocks: stock_id, ticker.
        ticker: The stock ticker to plot (e.g. "NVDA").
        start_date: ISO date string (e.g. "2024-01-01").
        end_date: ISO date string (e.g. "2026-03-30").

    Returns:
        A layered Altair chart with price line and sentiment bars.
    """
    prices = get_price_data(ticker, start_date, end_date)
    sentiment = build_daily_sentiment(
        reddit_analysis, reddit_posts, stocks, ticker
    )

    # Merge on date so both series align
    merged = prices.merge(sentiment, on="date", how="left")
    merged["date"] = pd.to_datetime(merged["date"])

    price_line = (
        alt.Chart(merged)
        .mark_line(color="#1f77b4")
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("close:Q", title="Close Price ($)"),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("close:Q", title="Price", format="$.2f"),
            ],
        )
    )

    sentiment_bars = (
        alt.Chart(merged.dropna(subset=["avg_sentiment"]))
        .mark_bar(opacity=0.5, width=2)
        .encode(
            x="date:T",
            y=alt.Y(
                "avg_sentiment:Q",
                title="Avg Sentiment",
                axis=alt.Axis(orient="right"),
                scale=alt.Scale(domain=[-1.0, 1.0]),
            ),
            color=alt.condition(
                alt.datum.avg_sentiment > 0,
                alt.value("#2ca02c"),
                alt.value("#d62728"),
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("avg_sentiment:Q",
                            title="Sentiment", format=".2f"),
                alt.Tooltip("post_count:Q", title="Posts"),
            ],
        )
    )

    chart = (
        alt.layer(price_line, sentiment_bars)
        .resolve_scale(y="independent")
        .properties(
            title=f"{ticker} — Price vs Reddit Sentiment",
            width=700,
            height=400,
        )
    )

    return chart
