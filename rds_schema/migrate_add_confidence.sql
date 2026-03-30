-- Migration: Add confidence column to rss_analysis table
-- This captures AI analyst confidence (High/Medium/Low) in sentiment scoring
-- Split into steps for faster execution on large tables

-- Step 1: Add column without constraint (instant on modern PostgreSQL)
ALTER TABLE rss_analysis 
ADD COLUMN confidence VARCHAR(10) DEFAULT 'Medium';

-- Step 2: Add the CHECK constraint separately (faster than combined)
ALTER TABLE rss_analysis 
ADD CONSTRAINT check_confidence 
CHECK (confidence IN ('High', 'Medium', 'Low'));

-- Step 3: Create index for query performance
CREATE INDEX idx_rss_analysis_confidence ON rss_analysis(confidence);

-- Verify the new column exists
SELECT column_name, data_type, column_default 
FROM information_schema.columns 
WHERE table_name = 'rss_analysis' AND column_name = 'confidence';
