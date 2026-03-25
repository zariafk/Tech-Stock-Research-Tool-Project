from app.ingest import load_data, convert_to_documents
from app.embed import get_embeddings
from app.vector_store import store_documents
from app.retrieve import retrieve_documents
from app.query import build_context, generate_answer


def run_rag_pipeline(data_path, user_query, ticker=None):
    data = load_data(data_path)
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


if __name__ == "__main__":
    query = "What was NVDA closing price yesterday?"
    answer = run_rag_pipeline("data/sample_alpaca.json", query, ticker="NVDA")
    print(answer)
