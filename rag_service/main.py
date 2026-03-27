"""
This is the main entry point for the RAG service. It demonstrates how to 
ingest data from various sources and answer a sample query using the RAG pipeline.
Mostly used for local testing currently.
"""

from app.pipeline import ingest_data, answer_query


def main():
    """to test RAG system locally"""
    ingest_data(source="alpaca", data_path="data/sample_alpaca.json")
    ingest_data(source="rss", data_path="data/sample_rss.json")
    ingest_data(source="reddit", data_path="data/sample_reddit.json")

    query = "What is happening with NVDA?"
    answer = answer_query(query, sources=["reddit"])
    print(answer)


if __name__ == "__main__":
    """calling testing main function"""
    main()
