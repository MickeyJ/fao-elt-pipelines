select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select production_value
from "fao"."public"."silver_production_cleaned"
where production_value is null



      
    ) dbt_internal_test