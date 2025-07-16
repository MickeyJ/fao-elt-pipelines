
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select area_code
from "fao"."public_silver"."silver_top_countries"
where area_code is null



  
  
      
    ) dbt_internal_test