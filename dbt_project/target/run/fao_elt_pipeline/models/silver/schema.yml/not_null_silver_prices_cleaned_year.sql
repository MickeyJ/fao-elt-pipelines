select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select year
from "fao"."public"."silver_prices_cleaned"
where year is null



      
    ) dbt_internal_test