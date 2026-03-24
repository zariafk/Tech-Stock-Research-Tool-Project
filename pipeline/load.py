"""
Uploads a DataFrame to S3 as
partitioned Parquet files via
awswrangler.

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

logger = logging.getLogger(__name__)

# TODO: Update keys with table names.
SOURCE_CATEGORY_MAP = {
    "<rss_table>": "RSS",
    "<alpaca_table>": "Alpaca",
    "<reddit_table>": "Reddit",
}

PARTITION_COLS = [
    "year",
    "month",
    "day",
]


def validate_columns(
    df: pd.DataFrame,
    required: list[str],
) -> None:
    """
    Raise ValueError if any required
    columns are absent from df.
    """
    for col in required:
        if col not in df.columns:
            raise ValueError(
                "Missing column: %s"
                % col
            )


class S3Uploader:
    """
    Writes a DataFrame to S3 as
    Partitioned Parquet files.
    """

    def __init__(
        self, bucket: str
    ) -> None:
        self.bucket = bucket

    def _build_s3_path(
        self, source: str
    ) -> str:
        """
        Build S3 URI mapped to its
        category (RSS/Alpaca/Reddit).
        """
        mapping = SOURCE_CATEGORY_MAP
        category = mapping.get(
            source, source
        )
        base = f"s3://{self.bucket}"
        return f"{base}/{category}/"

    def upload(
        self,
        df: pd.DataFrame,
        source: str,
        ticker_col: str,
    ) -> list[str]:
        """
        Upload df to S3, partitioned by
        ticker then year/month/day.
        """
        s3_path = self._build_s3_path(
            source
        )
        logger.info(
            "Uploading %d rows from"
            " '%s' to %s",
            len(df), source, s3_path,
        )
        part_cols = [
            ticker_col, *PARTITION_COLS
        ]
        validate_columns(df, part_cols)

        result = wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            partition_cols=part_cols,
            mode="append",
        )
        paths = result["paths"]
        logger.info(
            "Upload complete —"
            " %d file(s) for '%s'",
            len(paths), source,
        )
        return paths

    def list_files(
        self, source: str
    ) -> list[str]:
        """
        List all Parquet files in S3
        for the given source.
        """
        s3_path = self._build_s3_path(
            source
        )
        return wr.s3.list_objects(
            s3_path
        )
