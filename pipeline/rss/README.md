# RSS & News Pipeline

Automated extraction and sentiment analysis for the top 100 tech companies. Designed for hourly AWS Lambda execution.

## Features
- **Live/Historical**: Merges TechCrunch RSS (live) with Hacker News Algolia API (historical).
- **Smart Filter**: Pre-filters by keyword before calling OpenAI to minimize costs.
- **AI Enrichment**: Uses GPT-4o-mini for ticker-specific relevance scoring (0-10) and sentiment (-1.0 to 1.0).
- **Persistent Deduplication**: Generates MD5 article_id from links to prevent duplicate S3 entries across runs.

## Setup
1. Create .env: OPENAI_API_KEY=your_key
2. Install dependencies: pip install -r ../requirements.txt

## Usage
- Run Extraction: python rss_extract.py (saves test_results_*.csv locally for verification)
- Transform: rss_transform.py cleans and prepares data for storage

## Logic Flow
1. Extract: Pull raw articles from RSS and HN.
2. Deduplicate: Filter unique links.
3. Filter: Match against top_100_tech_companies.py.
4. Analyze: OpenAI determines Relevance & Sentiment.
5. Output: Standardized DataFrame for S3.

## Analysis
-     Act as: Senior Quant Analyst.
    Universe: {", ".join(tickers)}
    Input: "{entry['title']}" | "{entry['summary']}"

    Task: Score Relevance (0-10) and Sentiment (-1.0 to 1.0).

    Relevance Rubric:
    - 10: Direct idiosyncratic event (Earnings, M&A).
    - 8: Significant business news (New product, contract).
    - 7: Indirect impact (Competitor/Sector news).
    - <7: Ignore.

    Sentiment Rubric (Strictly use these values):
    - 1.0: Transformational positive news.
    - 0.5: Incremental/Standard positive news.
    - 0.0: Neutral/Mixed news.
    - -0.5: Incremental negative news.
    - -1.0: Catastrophic negative news.

    Output Format (JSON list):
    [{{
      "t": "TICKER",
      "r": [score],
      "s": [score],
      "why": "one sentence justification"
    }}]
    """