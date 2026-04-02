import os
import requests
import streamlit as st
import streamlit.components.v1 as components

RAG_API_URL = os.getenv("RAG_API_URL")


def ask_rag(question: str) -> str:
    """Send question to RAG API and return answer."""
    if not RAG_API_URL:
        return "RAG_API_URL is not set."

    try:
        response = requests.post(
            RAG_API_URL,
            json={"question": question},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("answer") or data.get("response") or str(data)

    except requests.exceptions.RequestException as e:
        return f"Error calling chatbot API: {e}"


def render_floating_sidebar_button():
    """Floating Ask AI button that clicks Streamlit's real sidebar toggle."""
    components.html(
        """
        <style>
            .ask-ai-btn {
                position: fixed;
                top: 50%;
                left: 0;
                transform: translateY(-50%);
                z-index: 999999;
                background: #0078ff;
                color: white;
                border: none;
                border-radius: 0 10px 10px 0;
                padding: 14px 10px;
                cursor: pointer;
                font-size: 13px;
                font-weight: 700;
                writing-mode: vertical-rl;
                text-orientation: mixed;
                letter-spacing: 1px;
                box-shadow: 2px 0 10px rgba(0,0,0,0.25);
            }

            .ask-ai-btn:hover {
                background: #005ecb;
            }
        </style>

        <button class="ask-ai-btn" id="ask-ai-btn">💬 Ask AI</button>

        <script>
            const clickRealSidebarButton = () => {
                const parentDoc = window.parent.document;

                const selectors = [
                    'button[data-testid="collapsedControl"]',
                    '[data-testid="stSidebarCollapseButton"] button',
                    'button[aria-label="Open sidebar"]',
                    'button[aria-label="Close sidebar"]'
                ];

                for (const selector of selectors) {
                    const btn = parentDoc.querySelector(selector);
                    if (btn) {
                        btn.click();
                        return true;
                    }
                }
                return false;
            };

            document.getElementById("ask-ai-btn").addEventListener("click", () => {
                clickRealSidebarButton();
            });
        </script>
        """,
        height=0,
        width=0,
    )


def render_chatbot():
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = [
            {
                "role": "assistant",
                "content": "Hello 👋\n \nI'm your StockSiphon assistant!\n \n\nUse the 'Market Data' tab for an overview of tech stock performance at the moment.\n \nUse the 'Search Company' tab to search for a specific stock and get a summary of its recent news and performance ✍️\n \nFinally, use me to ask any specific questions you have about tech stocks and information shown, and I'll do my best to help!\n \n\nP.S. I can't provide financial advice 😳 Always do your own research before making any investment decisions!"
            }
        ]

    # Keep sidebar width stable
    st.markdown(
        """
        <style>
        div[data-testid="stSidebar"] {
            min-width: 360px;
            max-width: 360px;
        }

        .chat-history-box {
            max-height: 400px;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 8px;
            margin-bottom: 12px;
        }

        .user-msg {
            align-self: flex-end;
            background: #0078ff;
            color: #fff;
            padding: 6px 10px;
            border-radius: 12px 12px 2px 12px;
            max-width: 85%;
            word-wrap: break-word;
            font-size: 0.9rem;
        }

        .bot-msg {
            align-self: flex-start;
            background: #f0f0f0;
            color: #111;
            padding: 6px 10px;
            border-radius: 12px 12px 12px 2px;
            max-width: 85%;
            word-wrap: break-word;
            font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    render_floating_sidebar_button()

    with st.sidebar:
        st.markdown("### 💬 StockSiphon Chatbot")
        st.divider()

        history_html = '<div class="chat-history-box">'
        for msg in st.session_state.chat_history:
            css_class = "user-msg" if msg["role"] == "user" else "bot-msg"
            history_html += f'<div class="{css_class}">{msg["content"]}</div>'
        history_html += "</div>"
        st.markdown(history_html, unsafe_allow_html=True)

        with st.form("chat_form", clear_on_submit=True):
            question = st.text_input(
                "Ask something",
                label_visibility="collapsed",
                placeholder="Type your question...",
            )
            submitted = st.form_submit_button("Send", use_container_width=True)

        if st.button("Clear chat 🗑️", use_container_width=True):
            st.session_state.chat_history = []
            st.rerun()

        if submitted and question.strip():
            st.session_state.chat_history.append(
                {"role": "user", "content": question}
            )
            answer = ask_rag(question)
            st.session_state.chat_history.append(
                {"role": "assistant", "content": answer}
            )
            st.rerun()
