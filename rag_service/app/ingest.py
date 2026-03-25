import json


def load_data(file_path):
    """Load stock data from a JSON file."""
    with open(file_path, "r") as f:
        return json.load(f)


def get_input_data(data_path=None, data=None):
    """Get input data either from a file path or directly from a provided data object."""
    if data is not None:
        return data

    if data_path is not None:
        return load_data(data_path)

    raise ValueError("Either data_path or data must be provided.")


def convert_to_documents(data):
    """Convert raw stock data into a list of documents with text and metadata."""
    documents = []
    required_metrics = ["open", "high", "low", "close", "volume"]

    for record in data:
        # ticker and timestamp are required
        if "ticker" not in record or "timestamp" not in record:
            continue

        # at least one metric must be present
        if not any(metric in record for metric in required_metrics):
            continue

        text = f"Stock {record['ticker']} at {record['timestamp']}"

        for required_metric in required_metrics:
            if required_metric in record:
                text += f", {required_metric} {record[required_metric]}"

        text += "."

        documents.append({
            "text": text,
            "metadata": {
                "ticker": record["ticker"],
                "timestamp": record["timestamp"]
            }
        })

    return documents
