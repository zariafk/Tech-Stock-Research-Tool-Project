"""Logger configuration for the ETL Process."""
import logging


def make_logger() -> logging.Logger:
    """Set up a logger for the script."""
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s : %(message)s",
        handlers=[
            logging.FileHandler("pipeline.log"),  # saves to file
            logging.StreamHandler()  # also logs to stdout (for CloudWatch/containers)
        ]
    )
    return logger


# logger = make_logger()
