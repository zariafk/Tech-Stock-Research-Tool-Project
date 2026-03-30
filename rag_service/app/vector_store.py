import os
import chromadb


def get_chroma_client():
    chroma_host = os.getenv("CHROMA_HOST")

    if not chroma_host:
        raise ValueError("CHROMA_HOST is not set")

    return chromadb.HttpClient(
        host=chroma_host,
        port=8000
    )


def get_collection():
    client = get_chroma_client()
    return client.get_or_create_collection(name="stock_data")


def build_document_id(doc: dict) -> str:
    metadata = doc["metadata"]
    source = metadata.get("source", "unknown")

    if source == "alpaca":
        if metadata.get("doc_type") == "live_bar":
            return f"alpaca_live_{metadata['ticker']}_{metadata['timestamp']}"
        return f"alpaca_{metadata['ticker']}_{metadata['date']}"

    if source == "rss":
        ticker = metadata.get("ticker", "unknown")
        timestamp = metadata.get("timestamp", "no_timestamp")
        link = metadata.get("link", "no_link")
        return f"rss_{ticker}_{timestamp}_{link}"

    if source == "reddit":
        timestamp = metadata.get("timestamp", "no_timestamp")
        url = metadata.get("url", "no_url")
        return f"reddit_{timestamp}_{url}"

    return f"{source}_{hash(doc['text'])}"


def store_documents(documents: list, embeddings: list) -> None:
    collection = get_collection()

    ids = [doc["id"] if "id" in doc else build_document_id(
        doc) for doc in documents]
    texts = [doc["text"] for doc in documents]
    metadatas = [doc["metadata"] for doc in documents]

    collection.upsert(
        ids=ids,
        documents=texts,
        embeddings=embeddings,
        metadatas=metadatas
    )
