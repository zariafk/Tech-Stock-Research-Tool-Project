import os
import requests
import streamlit as st

RAG_API_URL = os.getenv("RAG_API_URL")


def ask_rag(question: str) -> str:
    """Send question to RAG API and return answer."""
    if not RAG_API_URL:
        return "RAG_API_URL is not set."

    try:
        response = requests.post(
            f"{RAG_API_URL}",
            json={"question": question},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        # adjust these keys if your API returns something slightly different
        return data.get("answer") or data.get("response") or str(data)

    except requests.exceptions.RequestException as e:
        return f"Error calling chatbot API: {e}"


def render_chatbot():
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # floating button bottom right
    st.markdown(
        """
    <style>
    div[data-testid="stPopover"] {
        position: fixed !important;
        bottom: 20px !important;
        right: 20px !important;
        z-index: 999999 !important;
        overflow: visible !important;
    }

    div[data-testid="stPopover"] > div {
        overflow: visible !important;
    }

    div[data-testid="stPopover"] button {
        width: 56px !important;
        height: 56px !important;
        min-width: 56px !important;
        border-radius: 50% !important;
        padding: 0 !important;
        font-size: 24px !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    with st.popover("💬", use_container_width=False):
        st.markdown("### Chat")

        st.markdown('<div class="chat-history-box">', unsafe_allow_html=True)
        for msg in st.session_state.chat_history:
            if msg["role"] == "user":
                st.markdown(
                    f'<div class="user-msg">{msg["content"]}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="bot-msg">{msg["content"]}</div>',
                    unsafe_allow_html=True,
                )
        st.markdown("</div>", unsafe_allow_html=True)

        with st.form("chat_form", clear_on_submit=True):
            question = st.text_input(
                "Ask something", label_visibility="collapsed", placeholder="Type your question..."
            )
            submitted = st.form_submit_button("Send", use_container_width=True)

        if submitted and question.strip():
            st.session_state.chat_history.append(
                {"role": "user", "content": question}
            )

            answer = ask_rag(question)

            st.session_state.chat_history.append(
                {"role": "assistant", "content": answer}
            )
            st.rerun()
