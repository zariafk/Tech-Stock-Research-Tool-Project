from app.pipeline import ingest_data, answer_query

if __name__ == "__main__":
    ingest_data(source="alpaca", data_path="data/sample_alpaca.json")

    query = "What was NVDA closing price?"
    answer = answer_query(query, ticker="NVDA")
    print(answer)
