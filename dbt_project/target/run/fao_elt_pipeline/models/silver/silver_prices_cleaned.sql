
  
    

  create  table "fao"."public_silver"."silver_prices_cleaned__dbt_tmp"
  
  
    as
  
  (
    

WITH raw_prices AS (
    SELECT
        data,
        loaded_at
    FROM "fao"."bronze"."raw_prices"
),

flattened_prices AS (
    SELECT
        (data->>'area')::TEXT as country_name,
        (data->>'area_code')::TEXT as area_code,
        (data->>'area_code_m49')::TEXT as area_code_m49,
        (data->>'element')::TEXT as element_name,
        (data->>'element_code')::TEXT as element_code,
        (data->>'flag')::TEXT as flag,
        (data->>'item')::TEXT as item_name,
        (data->>'item_code')::TEXT as item_code,
        (data->>'item_code_cpc')::TEXT as item_code_cpc,
        (data->>'unit')::TEXT as unit,
        (data->>'value')::NUMERIC as price_value,
        (data->>'year')::INTEGER as year,
        loaded_at
    FROM raw_prices
)

SELECT
    
    md5(
            coalesce(cast(area_code as varchar), '') || '-' || 
            coalesce(cast(item_code as varchar), '') || '-' || 
            coalesce(cast(year as varchar), '')
    )
 as price_id,
    country_name,
    area_code,
    area_code_m49,
    element_name,
    element_code,
    flag,
    item_name,
    item_code,
    item_code_cpc,
    unit,
    price_value,
    year,
    -- Add data quality flags
    CASE
        WHEN price_value IS NULL OR price_value <= 0 THEN FALSE
        ELSE TRUE
    END as is_valid_price,
    -- Add standardized country names (handle special characters)
    CASE
        WHEN country_name LIKE '%Côte%' THEN 'Ivory Coast'
        WHEN country_name = 'China, mainland' THEN 'China'
        WHEN country_name = 'United States of America' THEN 'USA'
        WHEN country_name = 'United Kingdom of Great Britain and Northern Ireland' THEN 'UK'
        ELSE country_name
    END as country_name_standardized,
    loaded_at,
    CURRENT_TIMESTAMP as transformed_at
FROM flattened_prices
WHERE price_value IS NOT NULL
  AND price_value > 0
  AND year >= 1990  -- Focus on more recent data
  );
  