"""
Uploads an in-memory DataFrame to S3 as partitioned Parquet files using awswrangler.

Expected S3 layout
------------------
s3://<bucket>/<prefix>/<source>/
    year=2024/month=03/day=24/<uuid>.parquet
    ...
"""

import logging
import awswrangler as wr
import pandas as pd

logger = logging.getLogger(__name__)


class S3Uploader:
    """
    Writes a DataFrame directly to S3 as partitioned Parquet files.

    Parameters
    ----------
    bucket : Target S3 bucket name.
    prefix : Optional key prefix, e.g. "raw/".
    """

    def __init__(self, bucket: str, prefix: str = "") -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def upload(self, df: pd.DataFrame, source: str) -> list[str]:
        """
        Upload df to S3, partitioned by year/month/day derived from the 'at' column.

        Parameters
        ----------
        df     : DataFrame with year, month, day columns already added (via partition.add_time_partitions on the 'at' column).
        source : Source name used as the S3 prefix, e.g. "alpaca_bars", "news", "reddit_posts".

        Returns
        -------
        List of S3 URIs written.
        """
        parts = filter(None, [self.prefix, source])
        s3_path = f"s3://{self.bucket}/{'/'.join(parts)}/"

        logger.info("Uploading %d rows from '%s' to %s",
                    len(df), source, s3_path)

        result = wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            partition_cols=["year", "month", "day"],
            mode="append",
        )

        paths = result["paths"]
        logger.info("Upload complete — %d file(s) written for '%s'",
                    len(paths), source)

        return paths
