
    
    

with all_values as (

    select
        is_valid_production as value_field,
        count(*) as n_records

    from "fao"."public_silver"."silver_production_cleaned"
    group by is_valid_production

)

select *
from all_values
where value_field not in (
    'True','False'
)


