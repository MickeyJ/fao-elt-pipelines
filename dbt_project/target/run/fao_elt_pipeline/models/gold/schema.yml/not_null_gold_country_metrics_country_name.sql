select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select country_name
from "fao"."public"."gold_country_metrics"
where country_name is null



      
    ) dbt_internal_test