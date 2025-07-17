

WITH production_rankings AS (
    SELECT
        country_name_standardized,
        area_code,
        SUM(production_metric_tons) as total_production,
        COUNT(DISTINCT item_code) as product_diversity,
        COUNT(DISTINCT year) as years_of_data,
        AVG(production_metric_tons) as avg_annual_production,
        MAX(year) as latest_year
    FROM "fao"."public"."silver_production_cleaned"
    WHERE is_valid_production = TRUE
    GROUP BY country_name_standardized, area_code
),

price_rankings AS (
    SELECT
        country_name_standardized,
        area_code,
        AVG(price_value) as avg_price,
        COUNT(DISTINCT item_code) as items_with_prices,
        MAX(price_value) as max_price,
        MIN(price_value) as min_price
    FROM "fao"."public"."silver_prices_cleaned"
    WHERE is_valid_price = TRUE
    GROUP BY country_name_standardized, area_code
),

combined_rankings AS (
    SELECT
        COALESCE(pr.country_name_standardized, pc.country_name_standardized) as country_name,
        COALESCE(pr.area_code, pc.area_code) as area_code,
        pr.total_production,
        pr.product_diversity,
        pr.avg_annual_production,
        pc.avg_price,
        pc.items_with_prices,
        pc.max_price,
        -- Calculate rankings
        RANK() OVER (ORDER BY pr.total_production DESC NULLS LAST) as production_rank,
        RANK() OVER (ORDER BY pc.avg_price DESC NULLS LAST) as price_rank,
        RANK() OVER (ORDER BY pr.product_diversity DESC NULLS LAST) as diversity_rank
    FROM production_rankings pr
    FULL OUTER JOIN price_rankings pc
        ON pr.country_name_standardized = pc.country_name_standardized
        AND pr.area_code = pc.area_code
)

SELECT
    country_name,
    area_code,
    total_production,
    product_diversity,
    ROUND(avg_annual_production, 2) as avg_annual_production,
    ROUND(avg_price, 2) as avg_price_usd_per_tonne,
    items_with_prices,
    ROUND(max_price, 2) as max_price_usd_per_tonne,
    production_rank,
    price_rank,
    diversity_rank,
    -- Identify top performers
    CASE
        WHEN production_rank <= 10 THEN TRUE
        ELSE FALSE
    END as is_top_producer,
    CASE
        WHEN price_rank <= 10 THEN TRUE
        ELSE FALSE
    END as is_high_price_country,
    CASE
        WHEN diversity_rank <= 10 THEN TRUE
        ELSE FALSE
    END as is_diverse_producer,
    CURRENT_TIMESTAMP as calculated_at
FROM combined_rankings
WHERE country_name IS NOT NULL