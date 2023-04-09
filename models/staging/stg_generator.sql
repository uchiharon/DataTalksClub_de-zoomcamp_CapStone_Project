{{config(materialized='view')}}

-- Load the generation and fuel table
with generation_and_fuel as (
    select *, row_number() over(order by cast(plant_id as int)) as row_number from {{source('staging','generator')}}
),
