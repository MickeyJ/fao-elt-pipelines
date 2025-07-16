
    
    

with all_values as (

    select
        producer_category as value_field,
        count(*) as n_records

    from "fao"."public_gold"."gold_country_metrics"
    group by producer_category

)

select *
from all_values
where value_field not in (
    'Major Producer','Medium Producer','Small Producer'
)


