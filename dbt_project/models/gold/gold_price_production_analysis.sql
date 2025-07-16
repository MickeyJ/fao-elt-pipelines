{{
    config(
        materialized='table'
    )
}}

WITH item_production AS (
    SELECT
        item_name,
        item_code,
        year,
        SUM(production_metric_tons) as global_production,
        COUNT(DISTINCT area_code) as producing_countries
    FROM {{ ref('silver_production_cleaned') }}
    WHERE is_valid_production = TRUE
    GROUP BY item_name, item_code, year
),

item_prices AS (
    SELECT
        item_name,
        item_code,
        year,
        AVG(price_value) as avg_global_price,
        MAX(price_value) as max_price,
        MIN(price_value) as min_price,
        COUNT(DISTINCT area_code) as countries_with_prices
    FROM {{ ref('silver_prices_cleaned') }}
    WHERE is_valid_price = TRUE
    GROUP BY item_name, item_code, year
),

price_production_combined AS (
    SELECT
        COALESCE(ip.item_name, ipr.item_name) as item_name,
        COALESCE(ip.item_code, ipr.item_code) as item_code,
        COALESCE(ip.year, ipr.year) as year,
        ip.global_production,
        ip.producing_countries,
        ipr.avg_global_price,
        ipr.max_price,
        ipr.min_price,
        ipr.countries_with_prices,
        -- Calculate price-production metrics
        CASE
            WHEN ip.global_production > 0 AND ipr.avg_global_price > 0
            THEN ipr.avg_global_price * ip.global_production
            ELSE NULL
        END as market_value
    FROM item_production ip
    FULL OUTER JOIN item_prices ipr
        ON ip.item_code = ipr.item_code
        AND ip.year = ipr.year
)

SELECT
    item_name,
    item_code,
    -- Aggregate metrics across all years
    ROUND(AVG(global_production), 0) as avg_annual_production,
    ROUND(SUM(global_production), 0) as total_production_all_years,
    ROUND(AVG(avg_global_price), 2) as avg_price_all_years,
    ROUND(AVG(market_value), 0) as avg_annual_market_value,
    ROUND(SUM(market_value), 0) as total_market_value,
    -- Price volatility
    ROUND(AVG(max_price - min_price), 2) as avg_price_spread,
    ROUND(MAX(max_price), 2) as highest_price_recorded,
    -- Market concentration
    ROUND(AVG(producing_countries), 1) as avg_producing_countries,
    ROUND(AVG(countries_with_prices), 1) as avg_countries_with_prices,
    -- Categorization
    CASE
        WHEN AVG(global_production) > 10000000 THEN 'Major Commodity'
        WHEN AVG(global_production) > 1000000 THEN 'Medium Commodity'
        ELSE 'Minor Commodity'
    END as commodity_scale,
    CASE
        WHEN AVG(avg_global_price) > 2000 THEN 'Premium Product'
        WHEN AVG(avg_global_price) > 500 THEN 'Standard Product'
        ELSE 'Basic Product'
    END as price_tier,
    -- Data quality
    COUNT(DISTINCT year) as years_with_data,
    MIN(year) as first_year,
    MAX(year) as last_year,
    CURRENT_TIMESTAMP as calculated_at
FROM price_production_combined
WHERE item_name IS NOT NULL
GROUP BY item_name, item_code
HAVING COUNT(DISTINCT year) >= 5  -- Only include items with sufficient data
ORDER BY total_market_value DESC NULLS LAST