
    
    

select
    production_id as unique_field,
    count(*) as n_records

from "fao"."public"."silver_production_cleaned"
where production_id is not null
group by production_id
having count(*) > 1


