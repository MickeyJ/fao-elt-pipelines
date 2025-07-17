select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select region
from "fao"."public"."gold_regional_summary"
where region is null



      
    ) dbt_internal_test