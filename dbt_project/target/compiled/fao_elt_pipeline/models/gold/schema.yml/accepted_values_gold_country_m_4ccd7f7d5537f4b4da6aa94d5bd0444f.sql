
    
    

with all_values as (

    select
        production_trend as value_field,
        count(*) as n_records

    from "fao"."public"."gold_country_metrics"
    group by production_trend

)

select *
from all_values
where value_field not in (
    'Growing','Declining','Stable'
)


