{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['area_code', 'year'], 'type': 'btree'},
            {'columns': ['item_code'], 'type': 'btree'}
        ]
    )
}}

WITH raw_production AS (
    SELECT
        data,
        loaded_at
    FROM {{ source('bronze_fao', 'raw_food_balance') }}
),

flattened_production AS (
    SELECT
        (data->>'area')::TEXT as country_name,
        (data->>'area_code')::TEXT as area_code,
        (data->>'area_code_m49')::TEXT as area_code_m49,
        (data->>'element')::TEXT as element_name,
        (data->>'element_code')::TEXT as element_code,
        (data->>'flag')::TEXT as flag,
        (data->>'item')::TEXT as item_name,
        (data->>'item_code')::TEXT as item_code,
        (data->>'item_code_fbs')::TEXT as item_code_fbs,
        (data->>'unit')::TEXT as unit,
        (data->>'value')::NUMERIC as production_value,
        (data->>'year')::INTEGER as year,
        loaded_at
    FROM raw_production
)

SELECT
    {{ generate_surrogate_key(['area_code', 'element_code', 'item_code', 'year'])  }} as production_id,
    country_name,
    area_code,
    area_code_m49,
    element_name,
    element_code,
    flag,
    item_name,
    item_code,
    item_code_fbs,
    unit,
    production_value,
    year,
    -- Convert to metric tons for consistency
    CASE
        WHEN unit = '1000 t' THEN production_value * 1000
        ELSE production_value
    END as production_metric_tons,
    -- Add data quality flags
    CASE
        WHEN production_value IS NULL OR production_value < 0 THEN FALSE
        ELSE TRUE
    END as is_valid_production,
    -- Add standardized country names
    CASE
        WHEN country_name LIKE '%Côte%' THEN 'Ivory Coast'
        WHEN country_name = 'China, mainland' THEN 'China'
        WHEN country_name = 'United States of America' THEN 'USA'
        WHEN country_name = 'United Kingdom of Great Britain and Northern Ireland' THEN 'UK'
        ELSE country_name
    END as country_name_standardized,
    loaded_at,
    CURRENT_TIMESTAMP as transformed_at
FROM flattened_production
WHERE element_name = 'Production'  -- Focus on production data only
  AND production_value >= 0
  AND year >= 1990