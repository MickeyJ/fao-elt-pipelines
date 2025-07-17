
    
    

with all_values as (

    select
        price_tier as value_field,
        count(*) as n_records

    from "fao"."public"."gold_price_production_analysis"
    group by price_tier

)

select *
from all_values
where value_field not in (
    'Premium Product','Standard Product','Basic Product'
)


