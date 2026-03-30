import streamlit as st


def dashboard():

    # ---------------------------------------------------------------------------
    # App title
    # ---------------------------------------------------------------------------
    st.title("📈 Tech Stock Research")
    # ---------------------------------------------------------------------------
    # Tabs
    # ---------------------------------------------------------------------------
    tab_market, tab_ticker, tab_chatbot = st.tabs(
        ["Market Data", "Company Specific", "Chatbot"])

    # ── Tab 2: Specific company ──────────────────────────────────────────────────────────────
    with tab_ticker:
        st.subheader("Company Specific")
    # ── Tab 3: Chatbot ────────────────────────────────────────────────────────────
    with tab_chatbot:
        st.subheader("Chatbot")
