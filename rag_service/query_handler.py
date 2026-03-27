import json

from app.pipeline import answer_query


def lambda_handler(event, context) -> dict:
    """AWS Lambda handler for answering user queries using the RAG system."""
    try:
        body = event.get("body", {})

        if isinstance(body, str):
            body = json.loads(body)

        question = body.get("question")
        ticker = body.get("ticker")
        source = body.get("source")
        top_k = body.get("top_k")

        if not question:
            raise ValueError("Missing required field: question")

        sources = None
        if source and source != "all":
            sources = [source]

        answer = answer_query(
            user_query=question,
            ticker=ticker,
            sources=sources,
            top_k=top_k or 5
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "answer": answer
            })
        }

    except (ValueError, TypeError, json.JSONDecodeError) as e:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "status": "error",
                "type": type(e).__name__,
                "message": str(e)
            })
        }

    except RuntimeError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "type": "RuntimeError",
                "message": str(e)
            })
        }
