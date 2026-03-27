-- ============================================================
-- Tech Stock Research Tool — PostgreSQL Schema
-- ============================================================
DROP TABLE IF EXISTS story_stock CASCADE;
DROP TABLE IF EXISTS reddit_post CASCADE;
DROP TABLE IF EXISTS subreddit CASCADE;
DROP TABLE IF EXISTS rss_article CASCADE;
DROP TABLE IF EXISTS alpaca_history CASCADE;
DROP TABLE IF EXISTS alpaca_live CASCADE;
DROP TABLE IF EXISTS stock CASCADE;

-- Core lookup table for tracked equities
CREATE TABLE IF NOT EXISTS stock (
    stock_id    SERIAL PRIMARY KEY,
    ticker      VARCHAR(10)  NOT NULL UNIQUE,
    stock_name  VARCHAR(255) NOT NULL
);

-- RSS / Hacker News articles
CREATE TABLE IF NOT EXISTS rss_article (
    story_id        SERIAL PRIMARY KEY,
    title           VARCHAR(500)  NOT NULL,
    url             VARCHAR(1000) NOT NULL UNIQUE,
    summary         VARCHAR(5000),
    published_date  TIMESTAMP,
    source          VARCHAR(500)
);

-- Reddit posts
CREATE TABLE IF NOT EXISTS subreddit (
    subreddit_id          SERIAL PRIMARY KEY,
    subreddit_name        BIGINT,
    subreddit_subscribers BIGINT
);

CREATE TABLE IF NOT EXISTS reddit_post (
    post_id       SERIAL PRIMARY KEY,
    title         VARCHAR(500),
    contents      TEXT,
    flair         VARCHAR(255),
    score         INT,
    ups           INT,
    upvote_ratio  FLOAT,
    num_comments  INT,
    author        VARCHAR(255),
    created_at    TIMESTAMP,
    permalink     VARCHAR(1000),
    url           VARCHAR(1000),
    subreddit_id  BIGINT REFERENCES subreddit(subreddit_id)
);

-- Junction table: links stories (rss or reddit) to stocks with AI analysis
CREATE TABLE IF NOT EXISTS story_stock (
    story_id         INT NOT NULL,
    stock_id         INT NOT NULL REFERENCES stock(stock_id),
    sentiment_score  FLOAT,
    relevance_score  FLOAT,
    analysis         TEXT,
    story_type       VARCHAR(50) NOT NULL,
    PRIMARY KEY (story_id, stock_id, story_type)
);

-- Alpaca live snapshot (latest quote per stock)
CREATE TABLE IF NOT EXISTS alpaca_live (
    live_bar_id    SERIAL PRIMARY KEY,
    stock_id       INT NOT NULL REFERENCES stock(stock_id),
    latest_time  TIMESTAMPTZ NOT NULL,
    open           DECIMAL(14,6),
    high           DECIMAL(14,6),
    low            DECIMAL(14,6),
    close          DECIMAL(14,6),
    volume         BIGINT,
    trade_count    BIGINT,
    vwap           DECIMAL(14,6)
);


CREATE TABLE IF NOT EXISTS alpaca_history (
    history_bar_id SERIAL PRIMARY KEY,
    stock_id       INT NOT NULL REFERENCES stock(stock_id),
    bar_date       DATE NOT NULL,
    open           DECIMAL(14,6),
    high           DECIMAL(14,6),
    low            DECIMAL(14,6),
    close          DECIMAL(14,6),
    volume         BIGINT,
    trade_count    BIGINT,
    vwap           DECIMAL(14,6)
);

-- ============================================================
-- Seed stock table
-- ============================================================

INSERT INTO stock (ticker, stock_name) VALUES
    -- Mega Cap Tech
    ('AAPL', 'Apple'),
    ('MSFT', 'Microsoft'),
    ('NVDA', 'NVIDIA'),
    ('AMZN', 'Amazon'),
    ('GOOGL', 'Google'),
    ('META', 'Meta'),
    ('TSLA', 'Tesla'),
    -- Large Cap Platforms / Software
    ('NFLX', 'Netflix'),
    ('ADBE', 'Adobe'),
    ('CRM', 'Salesforce'),
    ('ORCL', 'Oracle'),
    ('NOW', 'ServiceNow'),
    ('CSCO', 'Cisco'),
    -- Semiconductors
    ('AVGO', 'Broadcom'),
    ('AMD', 'AMD'),
    ('INTC', 'Intel'),
    ('QCOM', 'Qualcomm'),
    ('AMAT', 'Applied Materials'),
    -- Cloud / SaaS / Data
    ('SNOW', 'Snowflake'),
    ('PLTR', 'Palantir'),
    ('SHOP', 'Shopify'),
    ('TEAM', 'Atlassian'),
    ('WDAY', 'Workday'),
    ('MDB', 'MongoDB'),
    -- Cybersecurity
    ('PANW', 'Palo Alto Networks'),
    ('CRWD', 'CrowdStrike'),
    ('FTNT', 'Fortinet'),
    -- Consumer Tech / Internet
    ('UBER', 'Uber'),
    ('ABNB', 'Airbnb'),
    ('SPOT', 'Spotify'),
    ('PYPL', 'PayPal'),
    -- Fintech / Emerging
    ('SQ', 'Block'),
    ('COIN', 'Coinbase'),
    ('HOOD', 'Robinhood'),
    ('GTLB', 'GitLab'),
    -- Hardware & Infrastructure
    ('DELL', 'Dell'),
    ('ANET', 'Arista Networks'),
    -- Design & AI Ops
    ('SNPS', 'Synopsys'),
    ('CDNS', 'Cadence'),
    ('U', 'Unity')
ON CONFLICT (ticker) DO NOTHING;