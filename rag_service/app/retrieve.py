from app.vector_store import collection
from app.embed import get_embeddings


def format_query(query, ticker=None):
    """Format the user query by optionally adding the ticker information."""
    if ticker:
        return f"Stock {ticker}. {query}"
    return query


def retrieve_documents(query, ticker=None, n_results=2):
    """Retrieve relevant documents from the ChromaDB collection based on the user query and optional ticker filter."""
    formatted_query = format_query(query, ticker)
    query_embedding = get_embeddings([formatted_query])[0]

    where_filter = {"ticker": ticker} if ticker else None

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=n_results,
        where=where_filter
    )

    return results
