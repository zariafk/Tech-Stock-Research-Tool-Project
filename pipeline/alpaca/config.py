"""Retrieve credentials from AWS Secrets Manager."""

import json
import boto3
from logger import logger

SECRET_NAME = "c22-trade-research-tool-secrets"
AWS_REGION = "eu-west-2"


def get_secret(secret_name: str = SECRET_NAME,
               region: str = AWS_REGION) -> dict:
    """Retrieve a secret from AWS Secrets Manager and return it as a dict."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    secrets = json.loads(response["SecretString"])
    logger.info("Loaded %d secret(s) from Secrets Manager", len(secrets))
    return secrets
