
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select country_name
from "fao"."public_silver"."silver_top_countries"
where country_name is null



  
  
      
    ) dbt_internal_test