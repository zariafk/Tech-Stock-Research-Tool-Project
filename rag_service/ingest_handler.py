import json

from app.pipeline import ingest_data


def lambda_handler(event, context):
    try:
        source = event.get("source")
        data = event.get("data")

        if not source:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "status": "error",
                    "message": "Missing required field: source"
                })
            }

        if not data or not isinstance(data, list):
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "status": "error",
                    "message": "Missing or invalid field: data"
                })
            }

        docs_before = len(data)

        ingest_data(source=source, data=data)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "source": source,
                "records_received": docs_before
            })
        }

    except (ValueError, TypeError) as e:
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
