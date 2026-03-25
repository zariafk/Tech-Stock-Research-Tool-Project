from app.ingest import load_data, convert_to_documents
from app.embed import get_embeddings
from app.vector_store import store_documents
from app.retrieve import retrieve_documents

data = load_data("data/sample_alpaca.json")
docs = convert_to_documents(data)

texts = [doc["text"] for doc in docs]
embeddings = get_embeddings(texts)

store_documents(docs, embeddings)

results = retrieve_documents("What was NVDA closing price?")

print(results["documents"])
print(results["metadatas"])
