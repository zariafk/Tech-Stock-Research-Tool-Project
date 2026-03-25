from app.ingest import load_data, convert_to_documents
from app.embed import get_embeddings

# load + convert
data = load_data("data/sample_alpaca.json")
docs = convert_to_documents(data)

# embeddings
texts = [doc["text"] for doc in docs]
embeddings = get_embeddings(texts)

print(len(embeddings))
print(len(embeddings[0]))
