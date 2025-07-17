
    
    

select
    region as unique_field,
    count(*) as n_records

from "fao"."public"."gold_regional_summary"
where region is not null
group by region
having count(*) > 1


