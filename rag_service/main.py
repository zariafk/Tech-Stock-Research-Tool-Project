from app.pipeline import run_rag_pipeline

if __name__ == "__main__":
    query = "What was NVDA closing price?"
    answer = run_rag_pipeline(
        user_query=query,
        ticker="NVDA",
        data_path="data/sample_alpaca.json"
    )
    print(answer)
