select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        commodity_scale as value_field,
        count(*) as n_records

    from "fao"."public"."gold_price_production_analysis"
    group by commodity_scale

)

select *
from all_values
where value_field not in (
    'Major Commodity','Medium Commodity','Minor Commodity'
)



      
    ) dbt_internal_test