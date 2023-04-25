{{ config(materialized="incremental") }}



SELECT

plant_id,					
nuclear_unit_id,					
operator_name,			
operator_id,						
census_region,			
nerc_region,											
reported_prime_mover,			
reported_fuel_type_code,			
aer_fuel_type_code,	
physical_unit_label,			
quantity,			
elec_quantity,			
mmbtuper_unit,			
tot_mmbtu,			
elec_mmbtu,			
netgen,			
year,			
month

FROM {{ref("stg_generation_and_fuel")}}

{% if is_incremental() %}

where year > (SELECT max(year) FROM {{ this }})

{% endif %}

{% if var('is_test_run', default=true) %}

limit 20

{% endif %}