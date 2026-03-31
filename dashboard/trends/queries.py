"""
queries.py

Contains SQL queries for the Tech Stock Research Tool.
"""

# Market data queries
HISTORY_QUERY = """
    SELECT
        s.ticker, s.stock_name,
        h.bar_date, h.open, h.high, h.low, h.close,
        h.volume, h.trade_count, h.vwap
    FROM alpaca_history h
    JOIN stock s ON s.stock_id = h.stock_id
    ORDER BY s.ticker, h.bar_date ASC
"""

LIVE_QUERY = """
    SELECT
        s.ticker, s.stock_name,
        l.latest_time, l.open, l.high, l.low, l.close,
        l.volume, l.trade_count, l.vwap
    FROM alpaca_live l
    JOIN stock s ON s.stock_id = l.stock_id
    ORDER BY l.latest_time DESC
"""

# Sentiment data query
SENTIMENT_QUERY = """
    SELECT s.ticker, s.stock_name, ra.sentiment_score, 'news' AS source,
           a.published_date AS published_at
    FROM rss_analysis ra
    JOIN stock s ON s.stock_id = ra.stock_id
    JOIN rss_article a ON a.story_id = ra.story_id

    UNION ALL

    SELECT s.ticker, s.stock_name, rda.sentiment_score, 'reddit' AS source,
           NULL::timestamptz AS published_at
    FROM reddit_analysis rda
    JOIN stock s ON s.stock_id = rda.stock_id
"""

# News articles query
NEWS_QUERY = """
    SELECT
        a.story_id, a.title, a.url, a.summary, a.published_date, a.source,
        ra.sentiment_score, ra.relevance_score, ra.analysis,
        s.ticker
    FROM rss_article a
    JOIN rss_analysis ra ON ra.story_id = a.story_id
    JOIN stock s         ON s.stock_id  = ra.stock_id
    ORDER BY a.published_date DESC
"""

# Reddit posts query
REDDIT_QUERY = """
    SELECT
        p.post_id, p.title, p.contents, p.flair, p.score,
        p.upvote_ratio, p.num_comments, p.author, p.created_at,
        p.permalink, sub.subreddit_name
    FROM reddit_post p
    JOIN subreddit sub ON sub.subreddit_id = p.subreddit_id
    ORDER BY p.created_at DESC
"""

# Return vs Volatility query
RETURN_VOLATILITY_QUERY = """
    SELECT
        h.stock_id,
        s.ticker,
        h.bar_date,
        h.close,
        h.volume,
        h.trade_count
    FROM alpaca_history h
    JOIN stock s ON h.stock_id = s.stock_id
    WHERE h.close IS NOT NULL
    ORDER BY s.ticker, h.bar_date
"""
