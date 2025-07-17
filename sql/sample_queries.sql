-- Sample Queries for FAO ELT Pipeline
-- Run these after the pipeline completes to explore your data

-- ========================================
-- BRONZE LAYER - Raw Data Exploration
-- ========================================

-- Check raw data structure
SELECT
    jsonb_pretty(data) as formatted_data,
    loaded_at
FROM bronze.raw_prices
LIMIT 1;

-- Count records by year in raw data
SELECT
    (data->>'year')::INT as year,
    COUNT(*) as record_count
FROM bronze.raw_prices
GROUP BY 1
ORDER BY 1 DESC;

-- ========================================
-- SILVER LAYER - Cleaned Data
-- ========================================

-- Top 10 countries by number of products
SELECT
    country_name_standardized,
    COUNT(DISTINCT item_name) as product_count,
    ROUND(AVG(price_value), 2) as avg_price
FROM silver_prices_cleaned
WHERE is_valid_price = TRUE
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- Price trends over time for a specific item
SELECT
    year,
    item_name,
    ROUND(AVG(price_value), 2) as avg_global_price,
    COUNT(DISTINCT country_name_standardized) as countries_reporting
FROM silver_prices_cleaned
WHERE item_name LIKE '%Wheat%'
  AND is_valid_price = TRUE
GROUP BY 1, 2
ORDER BY 1;

-- Countries with highest production diversity
SELECT
    *
FROM silver_top_countries
WHERE is_diverse_producer = TRUE
ORDER BY product_diversity DESC
LIMIT 10;

-- ========================================
-- GOLD LAYER - Business Analytics
-- ========================================

-- 1. Top producing countries with trends
SELECT
    country_name,
    producer_category,
    production_trend,
    TO_CHAR(total_production_metric_tons, 'FM999,999,999,999') as total_production,
    TO_CHAR(avg_annual_production_metric_tons, 'FM999,999,999') as avg_annual_production,
    years_with_data
FROM gold_country_metrics
ORDER BY total_production_metric_tons DESC
LIMIT 15;

-- 2. Most valuable agricultural commodities
SELECT
    item_name,
    commodity_scale,
    price_tier,
    TO_CHAR(total_market_value, 'FM$999,999,999,999') as total_market_value,
    TO_CHAR(avg_price_all_years, 'FM$999,999.99') as avg_price_per_tonne,
    avg_producing_countries
FROM gold_price_production_analysis
WHERE total_market_value IS NOT NULL
ORDER BY total_market_value DESC
LIMIT 10;

-- 3. Regional comparison
SELECT
    region,
    regional_scale,
    TO_CHAR(total_production_metric_tons, 'FM999,999,999,999') as total_production,
    TO_CHAR(total_market_value, 'FM$999,999,999,999') as market_value,
    CASE
        WHEN production_growth > 0 THEN '↑ ' || TO_CHAR(production_growth, 'FM999,999,999')
        WHEN production_growth < 0 THEN '↓ ' || TO_CHAR(ABS(production_growth), 'FM999,999,999')
        ELSE '→ Stable'
    END as growth_trend,
    total_producing_countries
FROM gold_regional_summary
ORDER BY total_production_metric_tons DESC;

-- 4. High-value countries (high prices, lower production)
SELECT
    gcm.country_name,
    gcm.avg_price_usd_per_tonne,
    gcm.total_production_metric_tons,
    gcm.price_category,
    stc.is_high_price_country
FROM gold_country_metrics gcm
JOIN silver_top_countries stc
    ON gcm.country_name = stc.country_name
WHERE gcm.price_category = 'High Value'
  AND gcm.producer_category != 'Major Producer'
ORDER BY gcm.avg_price_usd_per_tonne DESC
LIMIT 10;

-- 5. Production concentration analysis
WITH commodity_concentration AS (
    SELECT
        item_name,
        commodity_scale,
        avg_producing_countries,
        CASE
            WHEN avg_producing_countries < 10 THEN 'Highly Concentrated'
            WHEN avg_producing_countries < 30 THEN 'Moderately Concentrated'
            ELSE 'Widely Distributed'
        END as market_concentration
    FROM gold_price_production_analysis
    WHERE years_with_data >= 10
)
SELECT
    market_concentration,
    COUNT(*) as commodity_count,
    STRING_AGG(item_name, ', ' ORDER BY item_name) as example_commodities
FROM commodity_concentration
GROUP BY market_concentration
ORDER BY
    CASE market_concentration
        WHEN 'Highly Concentrated' THEN 1
        WHEN 'Moderately Concentrated' THEN 2
        ELSE 3
    END;

-- ========================================
-- DATA QUALITY CHECKS
-- ========================================

-- Check data coverage by year
SELECT
    'Prices' as data_type,
    MIN(year) as first_year,
    MAX(year) as last_year,
    COUNT(DISTINCT year) as years_covered,
    COUNT(*) as total_records
FROM silver_prices_cleaned
UNION ALL
SELECT
    'Production' as data_type,
    MIN(year) as first_year,
    MAX(year) as last_year,
    COUNT(DISTINCT year) as years_covered,
    COUNT(*) as total_records
FROM silver_production_cleaned;

-- Data quality summary
SELECT
    'Prices' as layer,
    COUNT(*) as total_records,
    SUM(CASE WHEN is_valid_price THEN 1 ELSE 0 END) as valid_records,
    ROUND(100.0 * SUM(CASE WHEN is_valid_price THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_percentage
FROM silver_prices_cleaned
UNION ALL
SELECT
    'Production' as layer,
    COUNT(*) as total_records,
    SUM(CASE WHEN is_valid_production THEN 1 ELSE 0 END) as valid_records,
    ROUND(100.0 * SUM(CASE WHEN is_valid_production THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_percentage
FROM silver_production_cleaned;