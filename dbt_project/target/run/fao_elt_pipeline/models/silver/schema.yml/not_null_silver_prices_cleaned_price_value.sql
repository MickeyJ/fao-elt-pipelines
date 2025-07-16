
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price_value
from "fao"."public_silver"."silver_prices_cleaned"
where price_value is null



  
  
      
    ) dbt_internal_test