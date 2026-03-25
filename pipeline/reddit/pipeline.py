from extract import extract_main


def main(subreddits: list[str]):
    """Main pipeline script to coordinate the full ETL process."""
    results = extract_main(subreddits)


if __name__ == "__main__":
    subreddits = ["trading", "stocks", "investing", "stockmarket", "valueinvesting", "options", "algotrading",
                  "semiconductors", "artificialinteligence", "cloudcomputing", "hardware", "wallstreetbets"]
    main(subreddits)
