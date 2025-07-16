
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region
from "fao"."public_gold"."gold_regional_summary"
where region is null



  
  
      
    ) dbt_internal_test