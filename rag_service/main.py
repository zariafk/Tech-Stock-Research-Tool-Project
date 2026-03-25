from app.pipeline import run_rag_pipeline

if __name__ == "__main__":
    """Example usage of the RAG pipeline."""
    query = "What was nvidia closing price? and at what time?"
    answer = run_rag_pipeline(
        user_query=query,
        ticker="NVDA",
        data_path="data/sample_alpaca.json"
    )
    print(answer)
