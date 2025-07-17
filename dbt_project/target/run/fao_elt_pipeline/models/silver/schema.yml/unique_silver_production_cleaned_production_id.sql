select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    production_id as unique_field,
    count(*) as n_records

from "fao"."public"."silver_production_cleaned"
where production_id is not null
group by production_id
having count(*) > 1



      
    ) dbt_internal_test