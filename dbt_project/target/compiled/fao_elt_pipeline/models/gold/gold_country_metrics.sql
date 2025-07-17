

WITH country_production AS (
    SELECT
        country_name_standardized as country_name,
        area_code,
        year,
        SUM(production_metric_tons) as annual_production,
        COUNT(DISTINCT item_code) as products_produced
    FROM "fao"."public"."silver_production_cleaned"
    WHERE is_valid_production = TRUE
    GROUP BY country_name_standardized, area_code, year
),

country_prices AS (
    SELECT
        country_name_standardized as country_name,
        area_code,
        year,
        AVG(price_value) as avg_annual_price,
        COUNT(DISTINCT item_code) as products_priced
    FROM "fao"."public"."silver_prices_cleaned"
    WHERE is_valid_price = TRUE
    GROUP BY country_name_standardized, area_code, year
),

country_trends AS (
    SELECT
        cp.country_name,
        cp.area_code,
        -- Production metrics
        SUM(cp.annual_production) as total_production_all_years,
        AVG(cp.annual_production) as avg_annual_production,
        MAX(cp.annual_production) as peak_annual_production,
        MIN(cp.annual_production) as min_annual_production,
        -- Calculate production growth rate (comparing first and last 3 years)
        AVG(CASE WHEN cp.year >= 2018 THEN cp.annual_production END) -
        AVG(CASE WHEN cp.year <= 1993 THEN cp.annual_production END) as production_change,
        -- Price metrics
        AVG(pc.avg_annual_price) as avg_price_all_years,
        MAX(pc.avg_annual_price) as max_price,
        MIN(pc.avg_annual_price) as min_price,
        -- Product diversity
        MAX(cp.products_produced) as max_products_produced,
        AVG(cp.products_produced) as avg_products_produced,
        -- Data availability
        COUNT(DISTINCT cp.year) as years_with_data,
        MIN(cp.year) as first_year,
        MAX(cp.year) as last_year
    FROM country_production cp
    LEFT JOIN country_prices pc
        ON cp.country_name = pc.country_name
        AND cp.area_code = pc.area_code
        AND cp.year = pc.year
    GROUP BY cp.country_name, cp.area_code
)





SELECT
    country_name,
    area_code,
    -- Production metrics
    ROUND(total_production_all_years, 0) as total_production_metric_tons,
    ROUND(avg_annual_production, 0) as avg_annual_production_metric_tons,
    ROUND(peak_annual_production, 0) as peak_production_metric_tons,
    ROUND(production_change, 0) as production_growth_metric_tons,
    CASE
        WHEN production_change > 0 THEN 'Growing'
        WHEN production_change < 0 THEN 'Declining'
        ELSE 'Stable'
    END as production_trend,
    -- Price metrics
    ROUND(avg_price_all_years, 2) as avg_price_usd_per_tonne,
    ROUND(max_price, 2) as max_price_usd_per_tonne,
    ROUND(min_price, 2) as min_price_usd_per_tonne,
    ROUND(max_price - min_price, 2) as price_volatility,
    -- Product diversity
    max_products_produced,
    ROUND(avg_products_produced, 1) as avg_products_produced,
    -- Classifications
    CASE
        WHEN total_production_all_years > 1000000 THEN 'Major Producer'
        WHEN total_production_all_years > 100000 THEN 'Medium Producer'
        ELSE 'Small Producer'
    END as producer_category,
    CASE
        WHEN avg_price_all_years > 1000 THEN 'High Value'
        WHEN avg_price_all_years > 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END as price_category,
    -- Metadata
    years_with_data,
    first_year,
    last_year,
    CURRENT_TIMESTAMP as calculated_at
FROM country_trends
WHERE total_production_all_years > 0
ORDER BY total_production_all_years DESC