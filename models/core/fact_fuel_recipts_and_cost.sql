{{ config(materialized="incremental") }}


SELECT 

			
plant_id,					
purchase_type,			
contract_expiration_date,			
energy_source,			
fuel_group,			
coalmine_type,			
coalmine_state,			
coalmine_county,			
coalmine_msha_id,			
coalmine_name,			
supplier,			
quantity,
average_heat_content,			
average_sulfur_content,			
average_ash_content,			
average_mercury_content,			
fuel_cost,			
regulated,			
operator_name,			
operator_id,			
primary_transportation_mode,			
secondary_transportation_mode,			
natural_gas_supply_contract_type,			
natural_gas_delivery_contract_type,			
moisture_content,			
chlorine_content,	
year,			
month

FROM {{ref("stg_fuel_recipts_and_cost")}}

{% if is_incremental() %}

where year > (SELECT max(year) FROM {{ this }})

{% endif %}


{% if var('is_test_run', default=true) %}

limit 20

{% endif %}