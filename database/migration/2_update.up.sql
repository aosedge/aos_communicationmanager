-- Up Migration Script

-- Add 'timestamp' column with a default value of the current timestamp
ALTER TABLE instances ADD COLUMN timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add 'cached' column with a default value of 0
ALTER TABLE instances ADD COLUMN cached INTEGER DEFAULT 0;
