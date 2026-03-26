import logging

from extract import extract_main
from transform import transform_main
from load import load_main


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SECRET_NAME = "reddit-pipeline/s3-credentials"

SUBREDDITS = [
    "trading", "stocks", "investing", "stockmarket",
    "valueinvesting", "options", "algotrading", "semiconductors",
    "artificialinteligence", "cloudcomputing", "hardware",
    "wallstreetbets",
]

FACT_POSTS_COLUMNS = [
    "id", "title", "selftext", "link_flair_text", "score",
    "ups", "upvote_ratio", "num_comments", "author",
    "created_utc", "permalink", "url", "subreddit_id",
]

DIM_SUBREDDITS_COLUMNS = [
    "subreddit_id", "subreddit", "subreddit_subscribers",
]

REQUIRED_COLUMNS = ["id", "title", "subreddit_id", "author"]


def main():
    """Main pipeline script to coordinate the full ETL process."""
    logger.info("Starting extract")
    raw_data = extract_main(SUBREDDITS, include_comments=False)

    logger.info("Starting transform")
    fact_posts, dim_subreddits = transform_main(
        raw_data,
        fact_columns=FACT_POSTS_COLUMNS,
        dim_columns=DIM_SUBREDDITS_COLUMNS,
        required_columns=REQUIRED_COLUMNS,
    )

    logger.info("Pipeline complete — %d posts, %d subreddits",
                len(fact_posts), len(dim_subreddits))

    logger.info("Starting load")
    load_main(
        {
            "fact_posts": fact_posts,
            "dim_subreddits": dim_subreddits,
        },
        secret_name=SECRET_NAME,
        dedupe_config={
            "fact_posts": "id",
            "dim_subreddits": "subreddit_id",
        },
    )

    logger.info("Pipeline complete")


if __name__ == "__main__":

    main()

    # bucket = "c22-tsrt-terraform-state"
    # key = "global/secrets_repository/terraform.tfstate"

# resource "aws_secretsmanager_secret" "reddit_pipeline" {
#     name = "reddit-pipeline/s3-credentials"
#     description = "S3 credentials and bucket name for the Reddit ETL pipeline"
# }
