from app.ingest import get_input_data, convert_to_documents
from app.embed import get_embeddings
from app.vector_store import store_documents
from app.retrieve import retrieve_documents
from app.query import build_context, generate_answer


def ingest_data(source, data_path=None, data=None):
    data = get_input_data(data_path=data_path, data=data)
    docs = convert_to_documents(data, source)

    texts = [doc["text"] for doc in docs]
    embeddings = get_embeddings(texts)

    store_documents(docs, embeddings)


def answer_query(user_query, ticker=None):
    results = retrieve_documents(user_query, ticker=ticker)

    documents = results.get("documents", [])
    if not documents or not documents[0]:
        return "I do not have enough information to answer that question."

    retrieved_docs = documents[0]
    print("RETRIEVED DOCS:", retrieved_docs)

    context = build_context(retrieved_docs)
    answer = generate_answer(user_query, context)
    return answer
