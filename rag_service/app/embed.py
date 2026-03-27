"""
This module contains functions for generating embeddings for documents 
using the OpenAI API.
"""

from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_embeddings(texts: list) -> list:
    """Get embeddings for a list of texts using OpenAI's embedding model."""
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=texts
    )

    return [item.embedding for item in response.data]
