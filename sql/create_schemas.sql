-- Create medallion architecture schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze layer tables (raw data)
CREATE TABLE IF NOT EXISTS bronze.raw_prices (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_url TEXT,
    api_endpoint TEXT
);

CREATE TABLE IF NOT EXISTS bronze.raw_food_balance (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_url TEXT,
    api_endpoint TEXT
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_raw_prices_loaded_at ON bronze.raw_prices(loaded_at);
CREATE INDEX IF NOT EXISTS idx_raw_food_balance_loaded_at ON bronze.raw_food_balance(loaded_at);

-- Grant permissions (adjust as needed)
GRANT USAGE ON SCHEMA bronze, silver, gold TO mickey;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze, silver, gold TO mickey;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze, silver, gold TO mickey;