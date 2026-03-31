"""
queries.py

Contains SQL queries for the Summary dashboard.
"""

# Stock search query
STOCK_SEARCH_QUERY = """
    SELECT stock_id, ticker, stock_name FROM stock
    WHERE LOWER(ticker) = %s OR LOWER(stock_name) LIKE %s
    LIMIT 1
"""

# Market data queries
MARKET_LATEST_QUERY = """
    SELECT close, open, high, low, volume, latest_time FROM alpaca_live
    WHERE stock_id = %s
    ORDER BY latest_time DESC LIMIT 1
"""

MARKET_HISTORY_QUERY = """
    SELECT bar_date, open, high, low, close, volume FROM alpaca_history
    WHERE stock_id = %s
    ORDER BY bar_date DESC LIMIT 30
"""

# News signals query
NEWS_SIGNALS_QUERY = """
    SELECT ra.sentiment_score, ra.relevance_score, ra.confidence, ra.analysis,
           rss.title, rss.summary, rss.published_date, rss.source
    FROM rss_analysis ra
    JOIN rss_article rss ON ra.story_id = rss.story_id
    WHERE ra.stock_id = %s
    ORDER BY rss.published_date DESC LIMIT 20
"""

# Social signals query
SOCIAL_SIGNALS_QUERY = """
    SELECT ra.sentiment_score, ra.relevance_score, ra.confidence, ra.analysis,
           rp.title, rp.score, rp.num_comments, rp.created_at, rp.url
    FROM reddit_analysis ra
    JOIN reddit_post rp ON ra.story_id = rp.post_id
    WHERE ra.stock_id = %s
    ORDER BY rp.created_at DESC LIMIT 20
"""

# Extended social query
EXTENDED_SOCIAL_QUERY = """
    SELECT ra.sentiment_score, ra.relevance_score, ra.confidence, ra.analysis,
           rp.post_id, rp.title, rp.contents, rp.score, rp.ups,
           rp.upvote_ratio, rp.num_comments, rp.created_at
    FROM reddit_analysis ra
    JOIN reddit_post rp ON ra.story_id = rp.post_id
    WHERE ra.stock_id = %s
    ORDER BY rp.created_at DESC LIMIT 200
"""

# Full market history query
FULL_MARKET_HISTORY_QUERY = """
    SELECT bar_date, open, high, low, close, volume, trade_count, vwap
    FROM alpaca_history
    WHERE stock_id = %s
    ORDER BY bar_date ASC LIMIT 365
"""
