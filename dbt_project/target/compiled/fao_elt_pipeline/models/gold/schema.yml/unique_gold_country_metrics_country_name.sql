
    
    

select
    country_name as unique_field,
    count(*) as n_records

from "fao"."public_gold"."gold_country_metrics"
where country_name is not null
group by country_name
having count(*) > 1


