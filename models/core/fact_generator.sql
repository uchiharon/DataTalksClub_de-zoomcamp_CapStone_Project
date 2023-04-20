{{config(materialized='table')}}


SELECT 

plant_id,			
operator_name,			
operator_id,						
census_region,			
nerc_region,			
sector_name,			
generator_id,			
reported_prime_mover,			
net_generator_generation,		
year,	
month

FROM {{ref('stg_generator')}}

{% if var('is_test_run', default=true) %}

limit 20

{% endif %}