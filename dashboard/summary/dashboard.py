"""
dashboard.py

Summary dashboard — stock deep-dive with market data, news, and social signals.
"""

import streamlit as st
from .queries import (
    get_stock_by_ticker_or_name,
    get_market_data,
    get_news_signals,
    get_social_signals,
    get_extended_social,
)
from .helpers import (
    render_market_section,
    render_news_section,
    render_social_section,
    render_divergence_section,
    render_visual_analytics,
)


def dashboard():
    """Render the full summary dashboard for a user-searched stock."""
    st.caption(
        "Consolidated view of market data, news signals, and community sentiment for specific stocks.")
    st.divider()

    col1, col2 = st.columns([3, 1])
    with col1:
        search_input = st.text_input(
            "Search by ticker or company name",
            placeholder="e.g., AAPL or Apple",
            key="stock_search",
        )
    with col2:
        st.write("")
        search_btn = st.button("Search", use_container_width=True)

    if not (search_btn or search_input):
        return

    if not search_input:
        st.warning("Please enter a stock ticker or company name.")
        return

    stock_result = get_stock_by_ticker_or_name(search_input)
    if not stock_result:
        st.error("Stock not found. Please check the ticker or company name.")
        return

    stock_id, ticker, company_name = stock_result
    st.divider()

    latest, history = get_market_data(stock_id)
    news = get_news_signals(stock_id)
    social = get_social_signals(stock_id)
    extended_social = get_extended_social(stock_id)

    st.header(f"Market Data — {ticker} ({company_name})")
    render_market_section(latest, history)
    st.divider()

    st.header("News & Market Signals")
    render_news_section(news)
    st.divider()

    st.header("Community Sentiment")
    render_social_section(social)
    render_divergence_section(news, social)
    st.divider()

    render_visual_analytics(history, extended_social, social, news)
    st.caption(
        "_Dashboard updated with live data from RDS. Refresh to see latest signals._")


if __name__ == "__main__":
    dashboard()
