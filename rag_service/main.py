from app.pipeline import ingest_data, answer_query

if __name__ == "__main__":
    ingest_data(source="alpaca", data_path="data/sample_alpaca.json")
    ingest_data(source="rss", data_path="data/sample_rss.json")

    query = "Why is NVDA price going up?"
    answer = answer_query(query, ticker="NVDA", sources=["rss"])
    print(answer)
