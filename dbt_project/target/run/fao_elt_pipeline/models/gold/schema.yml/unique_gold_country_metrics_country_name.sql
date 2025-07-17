select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    country_name as unique_field,
    count(*) as n_records

from "fao"."public"."gold_country_metrics"
where country_name is not null
group by country_name
having count(*) > 1



      
    ) dbt_internal_test