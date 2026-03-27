import streamlit as st


def dashboard():
    st.set_page_config(page_title="Plant Health Dashboard", layout="wide")
    st.title("🌿 Plant Health Dashboard")
    st.divider()

    # --- DASHBOARD SELECTION ---
    tab1, tab2 = st.tabs(["Live", "Historical"])
    with tab1:
        st.header("Live Data (Last 24h)")
        st.caption("Real-time insights from the last 24 hours.")
        live_dashboard()
    with tab2:
        st.header("Historical Trends (7d)")
        st.caption("Insights from the past 7 days.")
        hist_dashboard()


if __name__ == "__main__":
    dashboard()
