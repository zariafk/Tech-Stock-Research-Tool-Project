from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def build_context(retrieved_docs):
    return "\n".join(retrieved_docs)


def generate_answer(query, context):
    prompt = f"""
            You are a stock research assistant.

            Answer the user's question using only the context below.
            If the answer is not in the context, say you do not have enough information.

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
