#!/bin/bash
# Seed rss_article + story_stock tables with historical Hacker News data.
# Reads tickers from the stock table, runs extract → analysis → transform → load.
#
# Required env vars (set in .env or export before running):
#   DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
#   OPENAI_API_KEY

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env if present
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

echo "=== Seeding RSS tables ==="
python3 seed_rss_table.py
echo "=== Seed complete ==="
