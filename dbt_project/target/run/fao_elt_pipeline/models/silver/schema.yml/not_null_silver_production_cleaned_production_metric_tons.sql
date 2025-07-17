select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select production_metric_tons
from "fao"."public"."silver_production_cleaned"
where production_metric_tons is null



      
    ) dbt_internal_test