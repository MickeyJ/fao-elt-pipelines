
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test to ensure we have data for major countries
-- This test will fail if we don't have data for these important agricultural producers

SELECT country_name
FROM (
    VALUES
        ('China'),
        ('India'),
        ('USA'),
        ('Brazil'),
        ('Germany')
) AS required_countries(country_name)
WHERE country_name NOT IN (
    SELECT DISTINCT country_name_standardized
    FROM "fao"."public_silver"."silver_production_cleaned"
)
  
  
      
    ) dbt_internal_test