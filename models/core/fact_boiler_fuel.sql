{{ config(materialized="incremental") }}


SELECT

	
plant_id,				
operator_name,			
operator_id,					
census_region,			
nerc_region,						
boiler_id,			
reported_prime_mover,			
reported_fuel_type_code,			
physical_unit_label,	
quantity_of_boiler_fuel,			
heat_content_per_unit,			
sulfur_content,			
ash_content,			
year,			
month

FROM {{ref("stg_boiler_fuel")}}


{% if is_incremental() %}

where year > (SELECT max(year) FROM {{ this }})

{% endif %}




{% if var('is_test_run', default=true) %}

limit 20

{% endif %}