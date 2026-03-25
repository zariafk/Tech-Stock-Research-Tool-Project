from app.vector_store import collection
from app.embed import get_embeddings


def retrieve_documents(query, n_results=2):
    query_embedding = get_embeddings([query])[0]

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=n_results
    )

    return results
