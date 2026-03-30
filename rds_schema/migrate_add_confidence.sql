-- Migration: Add confidence column to rss_analysis table
-- This captures AI analyst confidence (High/Medium/Low) in sentiment scoring
-- Split into steps for faster execution on large tables

-- Step 1: Add column without constraint (instant on modern PostgreSQL)
ALTER TABLE reddit_analysis 
ADD COLUMN confidence VARCHAR(10) DEFAULT 'Unknown';

-- Step 2: Add the CHECK constraint separately (faster than combined)
ALTER TABLE reddit_analysis 
ADD CONSTRAINT check_confidence 
CHECK (confidence IN ('High', 'Medium', 'Low', 'Unknown'));

-- Step 3: Create index for query performance
CREATE INDEX idx_reddit_analysis_confidence ON reddit_analysis(confidence);

-- Verify the new column exists
SELECT column_name, data_type, column_default 
FROM information_schema.columns 
WHERE table_name = 'reddit_analysis' AND column_name = 'confidence';
