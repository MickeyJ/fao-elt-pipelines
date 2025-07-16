
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        regional_scale as value_field,
        count(*) as n_records

    from "fao"."public_gold"."gold_regional_summary"
    group by regional_scale

)

select *
from all_values
where value_field not in (
    'Major Agricultural Region','Medium Agricultural Region','Minor Agricultural Region'
)



  
  
      
    ) dbt_internal_test