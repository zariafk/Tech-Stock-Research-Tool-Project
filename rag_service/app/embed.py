import os
import json
import boto3

from openai import OpenAI


def get_secret(secret_name: str, region: str = "eu-west-2") -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


secret_name = os.getenv("SECRET_NAME")
secret_values = get_secret(secret_name)

client = OpenAI(api_key=secret_values["OPENAI_API_KEY"])


def get_embeddings(texts: list, batch_size: int = 20) -> list:
    """Get embeddings for a list of texts using OpenAI's embedding model in batches."""
    all_embeddings = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]

        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=batch
        )

        all_embeddings.extend([item.embedding for item in response.data])

    return all_embeddings
