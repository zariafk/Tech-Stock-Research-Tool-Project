"""Load transformed RSS articles to S3.

Strategy: Ticker + Day partitioned Parquet files.
  s3://{bucket}/rss/{ticker}/dt=YYYY-MM-DD/articles.parquet

On each Lambda run:
  1. For each ticker in the batch:
     a. Download existing Parquet from S3 (if it exists).
     b. Merge with the new batch.
     c. Deduplicate by article_id (cross-run deduplication).
     d. Re-upload the merged file.

Cost: 1 GET + 1 PUT per ticker per Lambda run — minimal S3 cost.
"""

import os
import io
from datetime import datetime, timezone
import pandas as pd
import boto3
from logger import logger

S3_BUCKET = os.environ.get('S3_BUCKET')
S3_PREFIX = 'rss'


def get_s3_key(ticker: str, date: datetime) -> str:
    """Build S3 key for ticker + date partition."""
    return f"{S3_PREFIX}/{ticker}/dt={date.strftime('%Y-%m-%d')}/articles.parquet"


def download_existing(client, bucket: str, key: str) -> pd.DataFrame:
    """Download existing Parquet from S3. Returns empty DataFrame if not found."""
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        existing = pd.read_parquet(io.BytesIO(response['Body'].read()))
        logger.info(
            "Downloaded %d existing articles from s3://%s/%s", len(existing), bucket, key)
        return existing
    except Exception as e:
        if hasattr(e, 'response') and e.response['Error']['Code'] == 'NoSuchKey':
            logger.info("No existing file at %s — starting fresh.", key)
            return pd.DataFrame()
        raise


def upload(client, bucket: str, key: str, df: pd.DataFrame) -> None:
    """Upload DataFrame as Parquet to S3."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info("Uploaded %d articles to s3://%s/%s", len(df), bucket, key)


def load(df: pd.DataFrame, bucket: str = None, date: datetime = None) -> int:
    """Load articles grouped by ticker. Merge with existing S3 partitions and deduplicate.

    Returns the total number of net new articles uploaded across all tickers.
    """
    bucket = bucket or S3_BUCKET
    if not bucket:
        raise ValueError("S3_BUCKET environment variable is not set.")

    if df.empty:
        logger.warning("No articles to load.")
        return 0

    date = date or datetime.now(timezone.utc)
    client = boto3.client('s3')
    total_net_new = 0

    # Group by ticker and upload separately
    for ticker, ticker_df in df.groupby('ticker'):
        key = get_s3_key(ticker, date)

        # Download existing partition for this ticker+date
        existing = download_existing(client, bucket, key)

        # Merge and deduplicate by article_id
        if not existing.empty:
            combined = pd.concat([existing, ticker_df], ignore_index=True)
        else:
            combined = ticker_df.copy()

        before = len(combined)
        combined = combined.drop_duplicates(
            subset=['article_id'], keep='first')
        net_new = len(combined) - len(existing)
        total_net_new += net_new

        logger.info(
            "[%s] Deduplication: %d duplicates removed. %d net new articles.", ticker, before - len(combined), net_new)

        # Re-upload merged partition
        upload(client, bucket, key, combined)

    return total_net_new


if __name__ == '__main__':
    # Local test: load from the most recent test CSV
    import glob
    import dotenv
    dotenv.load_dotenv()

    csv_files = sorted(glob.glob('test_results_*.csv'))
    if not csv_files:
        print("No test CSV found. Run rss_extract.py first.")
    else:
        latest = csv_files[-1]
        df = pd.read_csv(latest)
        print("Loaded %d articles from %s", len(df), latest)
        net_new = load(df)
        print("Net new articles uploaded to S3: %d", net_new)
