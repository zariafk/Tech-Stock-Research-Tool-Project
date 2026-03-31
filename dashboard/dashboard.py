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
    # App title
    # ---------------------------------------------------------------------------
    st.title("📈 Tech Stock Research")
    # ---------------------------------------------------------------------------
    # Tabs
    # ---------------------------------------------------------------------------
    tab_market, tab_ticker, tab_chatbot = st.tabs(
        ["Market Data", "Company Specific", "Chatbot"])

    render_chatbot()  # Render chatbot in the background so it's available across tabs

    # ── Tab 1: Market Data ──────────────────────────────────────────────────────────────
    with tab_market:
        trends_dashboard()
    # ── Tab 2: Specific company ──────────────────────────────────────────────────────────────
    with tab_ticker:
        summary_dashboard()
    # ── Tab 3: Chatbot ────────────────────────────────────────────────────────────
    with tab_chatbot:
        st.subheader("Chatbot")


if __name__ == "__main__":
    dashboard()
