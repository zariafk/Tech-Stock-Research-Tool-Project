"""
This module contains functions for retrieving relevant documents from 
the vector store based on a user query.
"""

import chromadb

from app.vector_store import get_collection
from app.embed import get_embeddings


def get_collection() -> chromadb.api.models.Collection.Collection:
    """Get the ChromaDB collection for stock data."""
    return client.get_or_create_collection(name="stock_data")


def format_query(query: str, ticker: str = None) -> str:
    """Format the user query to include the ticker if provided."""
    if ticker:
        return f"Stock {ticker}. {query}"
    return query


def retrieve_documents(query: str, ticker: str = None, source: str = None, n_results: int = 2) -> dict:
    """Retrieve documents from the vector store based on the query and optional filters."""
    formatted_query = format_query(query, ticker)
    query_embedding = get_embeddings([formatted_query])[0]

    filters = []

    if ticker:
        filters.append({"ticker": ticker})

    if source:
        filters.append({"source": source})

    if len(filters) == 1:
        where_filter = filters[0]
    elif len(filters) > 1:
        where_filter = {"$and": filters}
    else:
        where_filter = None

    collection = get_collection()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=n_results,
        where=where_filter
    )

    return results
