from app.ingest import get_input_data, convert_to_documents
from app.embed import get_embeddings
from app.vector_store import store_documents
from app.retrieve import retrieve_documents
from app.query import build_context, generate_answer


def run_rag_pipeline(user_query, ticker=None, data_path=None, data=None):
    """Run the full RAG pipeline: ingest, embed, store, retrieve, and generate answer."""
    data = get_input_data(data_path=data_path, data=data)

    docs = convert_to_documents(data)

    texts = [doc["text"] for doc in docs]
    embeddings = get_embeddings(texts)

    store_documents(docs, embeddings)

    results = retrieve_documents(user_query, ticker=ticker)
    retrieved_docs = results["documents"][0]

    if not retrieved_docs:
        return "I do not have enough information to answer that question."

    context = build_context(retrieved_docs)
    answer = generate_answer(user_query, context)
    return answer
