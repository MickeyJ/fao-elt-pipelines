
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select item_name
from "fao"."public_gold"."gold_price_production_analysis"
where item_name is null



  
  
      
    ) dbt_internal_test