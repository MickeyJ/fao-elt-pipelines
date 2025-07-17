select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select price_id
from "fao"."public"."silver_prices_cleaned"
where price_id is null



      
    ) dbt_internal_test