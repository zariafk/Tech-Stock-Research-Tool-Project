from app.ingest import convert_to_documents


def test_convert_to_documents():
    data = [
        {
            "ticker": "NVDA",
            "timestamp": "2026-03-25T14:30:00Z",
            "open": 120.5,
            "high": 125.2,
            "low": 119.8,
            "close": 124.7,
            "volume": 12345678
        }
    ]

    docs = convert_to_documents(data)

    assert len(docs) == 1
    assert docs[0]["metadata"]["ticker"] == "NVDA"
    assert "close 124.7" in docs[0]["text"]
