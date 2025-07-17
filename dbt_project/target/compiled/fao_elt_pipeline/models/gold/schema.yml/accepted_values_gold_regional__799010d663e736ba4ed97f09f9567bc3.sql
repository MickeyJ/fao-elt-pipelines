
    
    

with all_values as (

    select
        regional_scale as value_field,
        count(*) as n_records

    from "fao"."public"."gold_regional_summary"
    group by regional_scale

)

select *
from all_values
where value_field not in (
    'Major Agricultural Region','Medium Agricultural Region','Minor Agricultural Region'
)


