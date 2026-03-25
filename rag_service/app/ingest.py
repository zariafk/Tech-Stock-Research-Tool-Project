import json


def load_data(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def convert_to_documents(data):
    documents = []

    for record in data:
        text = (
            f"Stock {record['ticker']} at {record['timestamp']}: "
            f"open {record['open']}, high {record['high']}, "
            f"low {record['low']}, close {record['close']}, "
            f"volume {record['volume']}."
        )

        documents.append({
            "text": text,
            "metadata": {
                "ticker": record["ticker"],
                "timestamp": record["timestamp"]
            }
        })

    return documents
