
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        price_tier as value_field,
        count(*) as n_records

    from "fao"."public_gold"."gold_price_production_analysis"
    group by price_tier

)

select *
from all_values
where value_field not in (
    'Premium Product','Standard Product','Basic Product'
)



  
  
      
    ) dbt_internal_test