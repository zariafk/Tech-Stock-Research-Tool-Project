from app.ingest import load_data, convert_to_documents
from app.embed import get_embeddings
from app.vector_store import store_documents
from app.retrieve import retrieve_documents
from app.query import generate_answer

data = load_data("data/sample_alpaca.json")
docs = convert_to_documents(data)

texts = [doc["text"] for doc in docs]
embeddings = get_embeddings(texts)

store_documents(docs, embeddings)

query = "What was NVDA closing price at 2:30pm today?"
results = retrieve_documents(query)

retrieved_docs = results["documents"][0]
answer = generate_answer(query, retrieved_docs)

print(answer)
