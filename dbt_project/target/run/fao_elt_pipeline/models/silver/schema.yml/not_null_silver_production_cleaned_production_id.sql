
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select production_id
from "fao"."public_silver"."silver_production_cleaned"
where production_id is null



  
  
      
    ) dbt_internal_test