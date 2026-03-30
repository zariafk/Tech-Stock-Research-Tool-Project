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


def format_sources(retrieved_docs: list) -> str:
    """Format the sources of the retrieved documents for display."""
    sources = []

    for doc in retrieved_docs:
        meta = doc["metadata"]
        source = meta.get("source", "unknown")
        ticker = meta.get("ticker", "")
        date = meta.get("date") or meta.get(
            "timestamp") or meta.get("published_date", "unknown")
        url = meta.get("url")

        if url:
            sources.append(f"- {source.upper()} ({ticker}, {date}) → {url}")
        else:
            sources.append(f"- {source.upper()} ({ticker}, {date})")

    return "\n".join(sorted(set(sources)))


def get_task_type(query: str) -> str:
    q = query.lower().strip()

    if q.startswith("generate a summary for") or q.startswith("summarise") or q.startswith("summarize"):
        return "summary"

    return "chat"


def answer_query(user_query: str, ticker: str = None, sources: list = None, top_k: int = 5) -> str:
    """Answer a user query using the RAG pipeline."""
    if sources is None:
        sources = ["alpaca", "rss", "reddit"]

    retrieved_docs = []

    for source in sources:
        if source == "alpaca":
            n_results = min(top_k, 2)
        else:
            n_results = top_k

        results = retrieve_documents(
            user_query, ticker=ticker, source=source, n_results=n_results
        )
        docs = results.get("documents", [[]])[0]

        metadatas = results.get("metadatas", [[]])[0]

        for doc, meta in zip(docs, metadatas):
            retrieved_docs.append({"text": doc, "metadata": meta})

    if not retrieved_docs:
        return "I do not have enough information to answer that question."

    context = build_context(retrieved_docs)

    task_type = get_task_type(user_query)

    answer = generate_answer(user_query, context, task_type)

    sources_text = format_sources(retrieved_docs)
    return f"{answer}\n\nSources:\n{sources_text}"
