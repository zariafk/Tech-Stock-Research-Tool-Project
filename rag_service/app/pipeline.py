"""
This module defines the main pipeline functions for the RAG service, 
including data ingestion and query answering.
"""

from app.ingest import get_input_data, convert_to_documents
from app.embed import get_embeddings
from app.vector_store import store_documents
from app.retrieve import retrieve_documents
from app.query import build_context, generate_answer


def ingest_data(source: str, data_path: str = None, data: list = None) -> None:
    """Ingest data from the specified source and store it in the vector store."""
    data = get_input_data(data_path=data_path, data=data)
    docs = convert_to_documents(data, source)

    if not docs:
        return

    texts = [doc["text"] for doc in docs]
    embeddings = get_embeddings(texts)

    store_documents(docs, embeddings)


def answer_query(user_query: str, ticker: str = None, sources: list = None) -> str:
    """Answer a user query using the RAG pipeline."""
    if sources is None:
        sources = ["alpaca", "rss"]

    retrieved_docs = []

    for source in sources:
        if "price" in user_query.lower() or "close" in user_query.lower():
            n_results = 2 if source == "alpaca" else 1
        else:
            n_results = 1 if source == "alpaca" else 3

        results = retrieve_documents(
            user_query, ticker=ticker, source=source, n_results=n_results
        )
        docs = results.get("documents", [[]])[0]
        retrieved_docs.extend(docs)

    if not retrieved_docs:
        return "I do not have enough information to answer that question."

    print("RETRIEVED DOCS:", retrieved_docs)

    context = build_context(retrieved_docs)
    answer = generate_answer(user_query, context)
    return answer
