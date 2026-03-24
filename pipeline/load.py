"""
Uploads a DataFrame to S3 as partitioned Parquet files via awswrangler.

Expected S3 layout
------------------
s3://bucket/category/
    <ticker_col>=TSLA/
        year=2026/
            month=03/
                day=24/
                    <uuid>.parquet
"""

import logging
import awswrangler as wr
import pandas as pd

logger = logging.getLogger(__name__)  # TODO: Set up logging for this module

# TODO: Update key names to match actual table names
SOURCE_CATEGORY_MAP = {
    "<rss_table>": "RSS",
    "<alpaca_table>": "Alpaca",
    "<reddit_table>": "Reddit",
}

PARTITION_COLS = ["year", "month", "day"]


def validate_columns(df: pd.DataFrame, required: list[str]) -> None:
    """Raise ValueError if any required columns are absent from df."""
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")


class S3Uploader:
    """Writes a DataFrame to S3 as Hive-partitioned Parquet files."""

    def __init__(self, bucket: str, database: str) -> None:
        self.bucket = bucket
        self.database = database

    def _build_s3_path(self, source: str) -> str:
        """Build S3 URI mapped to its category (RSS/Alpaca/Reddit)."""
        category = SOURCE_CATEGORY_MAP.get(source, source)
        return f"s3://{self.bucket}/{category}/"

    def _write_parquet(self, df: pd.DataFrame,
                       s3_path: str,
                       source: str,
                       part_cols: list[str]) -> list[str]:
        """Write df to S3 as Parquet and register in Glue Catalog."""
        result = wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            partition_cols=part_cols,
            mode="append",
            database=self.database,
            table=source
        )
        return result["paths"]

    def upload(self, df: pd.DataFrame, source: str, ticker_col: str) -> list[str]:
        """Upload df to S3, partitioned by ticker then year/month/day."""
        part_cols = [ticker_col, *PARTITION_COLS]
        validate_columns(df, part_cols)
        s3_path = self._build_s3_path(source)
        logger.info("Uploading %d rows from '%s' to %s",
                    len(df), source, s3_path)
        paths = self._write_parquet(df, s3_path, source, part_cols)
        logger.info("Upload complete — %d file(s) for '%s'",
                    len(paths), source)
        return paths

    def list_files(self, source: str) -> list[str]:
        """List all Parquet files in S3 for the given source."""
        return wr.s3.list_objects(self._build_s3_path(source))
