import streamlit as st
from trends.dashboard import dashboard as trends_dashboard
from summary.dashboard import dashboard as summary_dashboard
from chatbot import render_chatbot


def dashboard():

    # ---------------------------------------------------------------------------
    # Page config
    # ---------------------------------------------------------------------------
    st.set_page_config(
        page_title="Tech Stock Research",
        page_icon="📈",
        layout="wide",
    )
    # ---------------------------------------------------------------------------
    # App branding with logo
    # ---------------------------------------------------------------------------
    st.warning(
        "⚠️  Disclaimer: This tool is not financial advice")
    col_logo, col_title = st.columns([1, 10])
    with col_logo:
        st.image("siphon_logo.png", width=300)
    with col_title:
        st.title("Stock Siphon Tool")
        st.caption("Chaos in, Clarity out")
    # ---------------------------------------------------------------------------
    # Tabs
    # ---------------------------------------------------------------------------
    tab_market, tab_ticker = st.tabs(
        ["Market Data", "Search Company"])

    render_chatbot()  # Render chatbot in the background so it's available across tabs

    # ── Tab 1: Market Data ──────────────────────────────────────────────────────────────
    with tab_market:
        trends_dashboard()
    # ── Tab 2: Specific company ──────────────────────────────────────────────────────────────
    with tab_ticker:
        summary_dashboard()


if __name__ == "__main__":
    dashboard()
