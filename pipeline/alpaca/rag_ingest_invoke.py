import json
import boto3
from logger import logger

lambda_client = boto3.client("lambda", region_name="eu-west-2")


def invoke_rag_ingest(source: str, data: list) -> None:
    logger.info("Invoking RAG ingest Lambda for source '%s' with %d records",
                source, len(data))
    payload = {
        "source": source,
        "data": data
    }

    response = lambda_client.invoke(
        FunctionName="c22-stocksiphon-rag-ingest-lambda",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8")
    )

    result = json.loads(response["Payload"].read().decode("utf-8"))

    if response["StatusCode"] != 200:
        raise RuntimeError(f"Failed to invoke RAG ingest Lambda: {result}")

    body = result.get("body")
    if body:
        body = json.loads(body)
        if body.get("status") != "success":
            raise RuntimeError(f"RAG ingest failed: {body}")
        else:
            logger.info("RAG ingest succeeded: %s", body)
