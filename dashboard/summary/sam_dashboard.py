"""Engagement vs Sentiment scatter chart."""

import altair as alt
import pandas as pd


def create_engagement_sentiment_scatter(
    reddit_posts: pd.DataFrame,
    subreddits: pd.DataFrame,
    reddit_analysis: pd.DataFrame,
    stocks: pd.DataFrame,
) -> alt.Chart:
    """Creates a scatter plot of engagement vs sentiment, coloured by subreddit.

    Args:
        reddit_posts: Columns include post_id, ups, num_comments, subreddit_id.
        subreddits: Columns include subreddit_id, subreddit_name.
        reddit_analysis: Columns include post_id, stock_id, sentiment_score,
                         relevance_score.
        stocks: Columns include stock_id, ticker.

    Returns:
        A layered Altair chart with num_comments on x-axis and
        sentiment_score on y-axis, sized by ups, coloured by subreddit.
    """
    # Join analysis with stock tickers
    analysis = reddit_analysis.merge(stocks, on="stock_id", how="left")

    # Join posts with subreddit names
    posts = reddit_posts.merge(subreddits, on="subreddit_id", how="left")

    # Combine: one row per post-ticker pair with engagement + sentiment
    df = analysis.merge(
        posts[["post_id", "ups", "num_comments", "subreddit_name"]],
        on="post_id",
        how="left",
    )

    # Build tooltip showing post context
    tooltip_fields = [
        alt.Tooltip("ticker:N", title="Ticker"),
        alt.Tooltip("subreddit_name:N", title="Subreddit"),
        alt.Tooltip("sentiment_score:Q", title="Sentiment", format=".1f"),
        alt.Tooltip("relevance_score:Q", title="Relevance"),
        alt.Tooltip("ups:Q", title="Upvotes"),
        alt.Tooltip("num_comments:Q", title="Comments"),
    ]

    scatter = (
        alt.Chart(df)
        .mark_circle(opacity=0.7)
        .encode(
            x=alt.X(
                "num_comments:Q",
                title="Comments",
                scale=alt.Scale(zero=False),
            ),
            y=alt.Y(
                "sentiment_score:Q",
                title="Sentiment Score",
                scale=alt.Scale(domain=[-1.0, 1.0]),
            ),
            size=alt.Size(
                "ups:Q",
                title="Upvotes",
                scale=alt.Scale(range=[30, 500]),
            ),
            color=alt.Color(
                "subreddit_name:N",
                title="Subreddit",
            ),
            tooltip=tooltip_fields,
        )
        .properties(
            title="Engagement vs Sentiment",
            width=700,
            height=450,
        )
    )

    # Horizontal line at sentiment = 0 for reference
    zero_line = (
        alt.Chart(pd.DataFrame({"y": [0]}))
        .mark_rule(color="grey", strokeDash=[4, 4], opacity=0.5)
        .encode(y="y:Q")
    )

    return scatter + zero_line
