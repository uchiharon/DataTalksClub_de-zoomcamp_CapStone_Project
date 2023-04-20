{{ config(materialized="table") }}


SELECT

plant_id,						
operator_name,			
operator_id,						
census_region,			
nerc_region,						
eia_sector_number,						
reported_prime_mover,			
reported_fuel_type_code,			
aer_fuel_type_code,			
physical_unit_label,			
quantity_of_energy_stored,	
elec_quantity,			
gross_gen,			
net_gen,			
month,			
year

FROM {{ref("stg_energy_storage")}}


{% if var('is_test_run', default=true) %}

limit 20

{% endif %}