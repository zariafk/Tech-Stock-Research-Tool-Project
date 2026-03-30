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


def get_embeddings(texts: list) -> list:
    """Get embeddings for a list of texts using OpenAI's embedding model."""
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=texts
    )

    return [item.embedding for item in response.data]
