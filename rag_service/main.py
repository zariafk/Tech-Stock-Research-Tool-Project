from app.pipeline import ingest_data, answer_query

if __name__ == "__main__":
    ingest_data(data_path="data/sample_alpaca.json")

    query = "Why is NVDA high right now?"
    answer = answer_query(query, ticker="NVDA")
    print(answer)
