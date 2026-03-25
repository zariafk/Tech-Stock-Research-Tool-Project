import chromadb

client = chromadb.PersistentClient(path="chroma_db")
collection = client.get_or_create_collection(name="stock_data")


def store_documents(documents, embeddings):
    """Store documents and their corresponding embeddings in the ChromaDB collection."""
    ids = [
        f"{doc['metadata']['ticker']}_{doc['metadata']['timestamp']}"
        for doc in documents
    ]
    texts = [doc["text"] for doc in documents]
    metadatas = [doc["metadata"] for doc in documents]

    collection.upsert(
        ids=ids,
        documents=texts,
        embeddings=embeddings,
        metadatas=metadatas
    )
