"""Reddit data load script — writes DataFrames to S3 as partitioned Parquet."""

import json
import logging
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd
import pyarrow.parquet as pq


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_secret(secret_name: str, region: str = "eu-west-2") -> dict:
    """Retrieves a secret from AWS Secrets Manager and returns it as a dict."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_s3_client(secret: dict) -> boto3.client:
    """Creates an S3 client using credentials from a secret dict."""
    return boto3.client(
        "s3",
        aws_access_key_id=secret["aws_access_key_id"],
        aws_secret_access_key=secret["aws_secret_access_key"],
        region_name=secret.get("region", "eu-west-2"),
    )


def build_s3_key(table_name: str, run_date: datetime) -> str:
    """Builds a date-partitioned S3 key."""
    return (
        f"{table_name}"
        f"/year={run_date.strftime('%Y')}"
        f"/month={run_date.strftime('%m')}"
        f"/day={run_date.strftime('%d')}"
        f"/data.parquet"
    )


def list_parquet_keys(
    s3_client: boto3.client,
    bucket: str,
    prefix: str,
) -> list[str]:
    """Lists all .parquet file keys under a given S3 prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    return keys


def read_column_from_s3_parquet(
    s3_client: boto3.client,
    bucket: str,
    key: str,
    column: str,
) -> pd.Series:
    """Reads a single column from a Parquet file stored in S3."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(BytesIO(response["Body"].read()), columns=[column])
    return table.to_pandas()[column]


def get_existing_ids(
    s3_client: boto3.client,
    bucket: str,
    table_name: str,
    id_column: str = "id",
) -> set[str]:
    """Collects all existing IDs for a table by scanning its Parquet files in S3."""
    keys = list_parquet_keys(s3_client, bucket, prefix=table_name + "/")

    if not keys:
        logger.info("No existing data found for %s", table_name)
        return set()

    all_ids = set()
    for key in keys:
        ids = read_column_from_s3_parquet(s3_client, bucket, key, id_column)
        all_ids.update(ids)

    logger.info("Found %d existing IDs in %s across %d files",
                len(all_ids), table_name, len(keys))
    return all_ids


def deduplicate(
    df: pd.DataFrame,
    existing_ids: set[str],
    id_column: str = "id",
) -> pd.DataFrame:
    """Removes rows whose ID already exists in the bucket."""
    before = len(df)
    df = df[~df[id_column].isin(existing_ids)]
    removed = before - len(df)

    if removed:
        logger.info("Removed %d duplicate rows, %d new rows remaining",
                    removed, len(df))
    return df


def upload_dataframe_to_s3(
    s3_client: boto3.client,
    df: pd.DataFrame,
    bucket: str,
    s3_key: str,
) -> None:
    """Converts a DataFrame to Parquet in memory and uploads it to S3."""
    buffer = df.to_parquet(index=False)

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer,
    )
    logger.info("Uploaded s3://%s/%s (%d rows)", bucket, s3_key, len(df))


def load_main(
    tables: dict[str, pd.DataFrame],
    *,
    secret_name: str,
    dedupe_config: dict[str, str] | None = None,
    region: str = "eu-west-2",
) -> None:
    """Loads all tables to S3 as date-partitioned Parquet files.

    Args:
        tables: Mapping of table name to DataFrame.
        secret_name: Name of the secret in AWS Secrets Manager.
        dedupe_config: Optional mapping of table name to the column
                       used for deduplication, e.g. {"fact_posts": "id"}.
        region: AWS region for Secrets Manager.
    """
    secret = get_secret(secret_name, region)
    s3_client = get_s3_client(secret)
    bucket = secret["bucket_name"]
    run_date = datetime.now(timezone.utc)

    if dedupe_config is None:
        dedupe_config = {}

    for table_name, df in tables.items():
        if df.empty:
            logger.warning("Skipping empty table: %s", table_name)
            continue

        # Deduplicate if configured for this table
        if table_name in dedupe_config:
            id_column = dedupe_config[table_name]
            existing_ids = get_existing_ids(
                s3_client, bucket, table_name, id_column
            )
            df = deduplicate(df, existing_ids, id_column)

            if df.empty:
                logger.info(
                    "No new rows for %s after deduplication", table_name)
                continue

        s3_key = build_s3_key(table_name, run_date)
        upload_dataframe_to_s3(s3_client, df, bucket, s3_key)

    logger.info("Load complete — %d tables processed", len(tables))
