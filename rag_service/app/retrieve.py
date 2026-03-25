from app.vector_store import collection
from app.embed import get_embeddings


def retrieve_documents(query, ticker=None, n_results=2):
    query_embedding = get_embeddings([query])[0]

    where_filter = {"ticker": ticker} if ticker else None

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=n_results,
        where=where_filter
    )

    return results
