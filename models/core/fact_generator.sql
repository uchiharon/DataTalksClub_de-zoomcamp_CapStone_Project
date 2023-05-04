{{ config(materialized="incremental") }}


SELECT 

plant_id,			
operator_name,			
operator_id,						
census_region,			
nerc_region,						
generator_id,			
reported_prime_mover,			
net_generator_generation,		
year,	
month

FROM {{ref('stg_generator')}}

{% if is_incremental() %}

where year > (SELECT max(year) FROM {{ this }})

{% endif %}

{% if var('is_test_run', default=true) %}

limit 20

{% endif %}