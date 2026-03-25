from app.ingest import load_data, convert_to_documents
from app.embed import get_embeddings
from app.vector_store import store_documents

data = load_data("data/sample_alpaca.json")
docs = convert_to_documents(data)

texts = [doc["text"] for doc in docs]
embeddings = get_embeddings(texts)

store_documents(docs, embeddings)

print("stored successfully")
