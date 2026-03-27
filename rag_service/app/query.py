"""
This module contains functions for building the context from retrieved documents 
and generating answers to user queries using the OpenAI API.
"""

from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def build_context(retrieved_docs) -> str:
    """Build a context string from the retrieved documents."""
    return "\n".join(retrieved_docs)


def generate_answer(query, context) -> str:
    """Generate an answer to the user's query based on the provided context."""
    prompt = f"""
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

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content
