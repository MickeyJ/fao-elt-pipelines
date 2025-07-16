
    
    

with all_values as (

    select
        is_valid_price as value_field,
        count(*) as n_records

    from "fao"."public_silver"."silver_prices_cleaned"
    group by is_valid_price

)

select *
from all_values
where value_field not in (
    'True','False'
)


