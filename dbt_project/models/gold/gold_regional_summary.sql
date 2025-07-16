{{
    config(
        materialized='table'
    )
}}

WITH country_regions AS (
    -- Simple regional classification based on country names
    -- In production, you'd use a proper country-region mapping table
    SELECT
        country_name_standardized,
        area_code,
        CASE
            WHEN country_name_standardized IN ('China', 'India', 'Japan', 'Republic of Korea',
                                               'Thailand', 'Pakistan', 'Bangladesh', 'Myanmar') THEN 'Asia'
            WHEN country_name_standardized IN ('USA', 'Canada', 'Mexico') THEN 'North America'
            WHEN country_name_standardized IN ('Brazil', 'Argentina', 'Chile', 'Colombia',
                                               'Peru', 'Ecuador', 'Venezuela (Bolivarian Republic of)',
                                               'Paraguay', 'Uruguay', 'Bolivia (Plurinational State of)') THEN 'South America'
            WHEN country_name_standardized IN ('Germany', 'France', 'UK', 'Italy', 'Spain',
                                               'Poland', 'Netherlands (Kingdom of the)', 'Belgium',
                                               'Greece', 'Portugal', 'Sweden', 'Austria',
                                               'Denmark', 'Finland', 'Norway', 'Switzerland') THEN 'Europe'
            WHEN country_name_standardized IN ('Nigeria', 'Egypt', 'South Africa', 'Kenya',
                                               'Ethiopia', 'Morocco', 'Tunisia', 'Algeria',
                                               'Ghana', 'Ivory Coast', 'Senegal') THEN 'Africa'
            WHEN country_name_standardized IN ('Australia', 'New Zealand', 'Fiji') THEN 'Oceania'
            WHEN country_name_standardized IN ('Saudi Arabia', 'Iran (Islamic Republic of)',
                                               'Iraq', 'Syrian Arab Republic', 'Yemen',
                                               'United Arab Emirates', 'Oman') THEN 'Middle East'
            ELSE 'Other'
        END as region
    FROM (
        SELECT DISTINCT country_name_standardized, area_code
        FROM {{ ref('silver_production_cleaned') }}
        UNION
        SELECT DISTINCT country_name_standardized, area_code
        FROM {{ ref('silver_prices_cleaned') }}
    ) countries
),

regional_production AS (
    SELECT
        cr.region,
        p.year,
        p.item_name,
        SUM(p.production_metric_tons) as regional_production,
        COUNT(DISTINCT p.area_code) as countries_producing,
        AVG(p.production_metric_tons) as avg_country_production
    FROM {{ ref('silver_production_cleaned') }} p
    JOIN country_regions cr
        ON p.country_name_standardized = cr.country_name_standardized
    WHERE p.is_valid_production = TRUE
    GROUP BY cr.region, p.year, p.item_name
),

regional_prices AS (
    SELECT
        cr.region,
        pr.year,
        pr.item_name,
        AVG(pr.price_value) as avg_regional_price,
        MAX(pr.price_value) as max_regional_price,
        MIN(pr.price_value) as min_regional_price,
        COUNT(DISTINCT pr.area_code) as countries_with_prices
    FROM {{ ref('silver_prices_cleaned') }} pr
    JOIN country_regions cr
        ON pr.country_name_standardized = cr.country_name_standardized
    WHERE pr.is_valid_price = TRUE
    GROUP BY cr.region, pr.year, pr.item_name
)

SELECT
    COALESCE(rp.region, rpr.region) as region,
    -- Production metrics
    ROUND(SUM(rp.regional_production), 0) as total_production_metric_tons,
    ROUND(AVG(rp.regional_production), 0) as avg_annual_production,
    COUNT(DISTINCT rp.countries_producing) as total_producing_countries,
    COUNT(DISTINCT rp.item_name) as products_produced,
    -- Price metrics
    ROUND(AVG(rpr.avg_regional_price), 2) as avg_price_usd_per_tonne,
    ROUND(MAX(rpr.max_regional_price), 2) as highest_price_recorded,
    ROUND(AVG(rpr.max_regional_price - rpr.min_regional_price), 2) as avg_price_volatility,
    -- Market value
    ROUND(SUM(
        CASE
            WHEN rp.regional_production > 0 AND rpr.avg_regional_price > 0
            THEN rp.regional_production * rpr.avg_regional_price
            ELSE 0
        END
    ), 0) as total_market_value,
    -- Growth metrics (comparing recent vs early years)
    ROUND(
        AVG(CASE WHEN rp.year >= 2018 THEN rp.regional_production END) -
        AVG(CASE WHEN rp.year <= 1995 THEN rp.regional_production END),
        0
    ) as production_growth,
    -- Regional characteristics
    CASE
        WHEN SUM(rp.regional_production) > 100000000 THEN 'Major Agricultural Region'
        WHEN SUM(rp.regional_production) > 10000000 THEN 'Medium Agricultural Region'
        ELSE 'Minor Agricultural Region'
    END as regional_scale,
    -- Data coverage
    COUNT(DISTINCT COALESCE(rp.year, rpr.year)) as years_with_data,
    MIN(COALESCE(rp.year, rpr.year)) as first_year,
    MAX(COALESCE(rp.year, rpr.year)) as last_year,
    CURRENT_TIMESTAMP as calculated_at
FROM regional_production rp
FULL OUTER JOIN regional_prices rpr
    ON rp.region = rpr.region
    AND rp.year = rpr.year
    AND rp.item_name = rpr.item_name
WHERE COALESCE(rp.region, rpr.region) != 'Other'
GROUP BY COALESCE(rp.region, rpr.region)
ORDER BY total_production_metric_tons DESC