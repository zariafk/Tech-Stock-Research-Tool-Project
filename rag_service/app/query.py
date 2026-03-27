"""
This module contains functions for building the context from retrieved documents 
and generating answers to user queries using the OpenAI API.
"""

from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def build_context(retrieved_docs: list) -> str:
    """Build a context string from the retrieved documents."""
    return "\n".join([doc["text"] for doc in retrieved_docs])


def generate_chat_prompt(query: str, context: str) -> str:
    """Generate a prompt for the language model based on the user query and context."""
    return f"""
            You are a stock research assistant.

            Answer the user's question using only the context below.
            If the answer is not in the context, say you do not have enough information.

            Give precise information, with contextual details, like date and time where appropriate, 
            and avoid making assumptions or adding information not present in the context.

            Never give financial advice or stock recommendations. Make it clear to the user that you cannot 
            give financial advice if prompted. Focus on providing factual information based on the context.

            Context:
            {context}

            Question:
            {query}
            """


def generate_summary_prompt(query: str, context: str) -> str:
    """Generate a prompt for the language model to create a summary based on the context."""
    return f"""
    You are a stock research assistant.

    Generate a concise, structured summary based on the user's request.

    User request:
    {query}

    Context:
    {context}

    The summary should:
    - focus on the most relevant points
    - include key events, sentiment, and price information if present
    - include dates/times where relevant
    - not include information not in the context

    Never give financial advice. If you are asked to give financial advice, clearly state that you cannot provide it. 
    Ensure the summary is in plain english, and encapsulates factual information.
    """


def generate_prompt(query: str, context: str, task_type: str) -> str:
    """Generate a prompt for the language model based on the task type."""
    if task_type == "summary":
        return generate_summary_prompt(query, context)
    else:
        return generate_chat_prompt(query, context)


def generate_answer(query: str, context: str, task_type: str) -> str:
    """Generate an answer to the user's query based on the provided context."""
    prompt = generate_prompt(query, context, task_type)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content
