# Stock Intelligence Dashboard

A comprehensive Streamlit dashboard that consolidates real-time market data, news signals, and community sentiment for any stock in the tech sector.

## Features

- **Search by Ticker or Company Name** — Find any tracked stock instantly
- **Live Market Data** — Current price, day's range, volume, and recent trend analysis
- **News Signals** — RSS articles with sentiment scoring, ranked by relevance
- **Community Sentiment** — Reddit discussions tracked and analyzed for bullish/bearish signals
- **Economic Context** — Plain English summary of what's happening and why it matters
- **Single Dashboard View** — All signals in one place for quick decision-making

## Setup

### Prerequisites

1. PostgreSQL RDS database with schema initialized (see `rds_schema/schema.sql`)
2. Environment variables configured:
   - `DB_HOST` — RDS endpoint
   - `DB_PORT` — PostgreSQL port (default: 5432)
   - `DB_USER` — Database username
   - `DB_PASSWORD` — Database password
   - `DB_NAME` — Database name

3. Virtual environment created and activated

### Installation

```bash
cd dashboard/summary
pip install -r requirements.txt
```

### Running the Dashboard

```bash
streamlit run dashboard.py
```

The dashboard will open in your browser at `http://localhost:8501`.

## Dashboard Sections

### 1. Market Data
Shows the latest price quote including:
- Current price with change from open
- Today's high/low range
- Trading volume
- Trend indicator (uptrend, downtrend, or sideways)

### 2. News & Market Signals
Displays RSS/news coverage with:
- Overall sentiment score (positive, neutral, negative)
- Average relevance score
- Recent articles with summaries and AI analysis
- Breakdown of positive vs. negative coverage

### 3. Community Sentiment
Tracks Reddit discussions with:
- Community sentiment score
- Engagement metrics (total comments)
- Top posts with bullish/bearish classifications
- AI-powered community takeaways

### 4. Economic Context
Provides plain English interpretation of:
- How the stock fits into broader trends
- What the data signals mean
- Data sources and confidence indicators

## Data Flow

```
RDS Database
├── Stock table (ticker + company name)
├── alpaca_live (latest OHLCV data)
├── alpaca_history (30-day historical data)
├── rss_article + rss_analysis (news with sentiment)
└── reddit_post + reddit_analysis (social with sentiment)
         ↓
    Dashboard Queries
         ↓
    Streamlit UI (consolidated view)
```

## Customization

To add more stocks to the dashboard, insert rows into the `stock` table in your RDS database:

```sql
INSERT INTO stock (ticker, stock_name) VALUES ('TICKER', 'Company Name');
```

The dashboard tracks all stocks in the table and supports fuzzy matching.

## Future Enhancements

- Time-series price chart with overlaid sentiment signals
- Comparative analysis across multiple stocks
- Custom alerts based on sentiment thresholds
- Export reports to PDF/CSV
- Real-time data refresh without page reload

## Troubleshooting

**"Failed to connect to database"**
- Check that DB_HOST, DB_USER, DB_PASSWORD, and DB_NAME are set correctly
- Verify RDS security groups allow your IP
- Ensure `sslmode="require"` is compatible with your RDS configuration

**"Stock not found"**
- Verify the stock is in the `stock` table in RDS
- Check the ticker symbol is correct (e.g., AAPL not APL)

**No data showing for a stock**
- Ensure the stock has been ingested by the data pipeline (alpaca, RSS, Reddit)
- Check that the pipeline containers are running and populating the database

## Architecture Notes

- **Frontend:** Streamlit for fast, interactive UI
- **Backend:** PostgreSQL RDS for data persistence
- **Data Source:** Alpaca API (market data), RSS feeds (news), Reddit API (social)
- **Enrichment:** AI sentiment scoring on all articles and posts
