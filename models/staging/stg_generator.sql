{{config(materialized='view')}}

-- Load the generator table

SELECT 
NULLIF(plant_id, '.') AS plant_id,			
NULLIF(combined_heat_and__power_plant, '.') AS combined_heat_and__power_plant,			
NULLIF(plant_name, '.') AS plant_name,			
NULLIF(operator_name, '.') AS operator_name,			
NULLIF(operator_id, '.') AS operator_id,			
NULLIF(plant_state, '.') AS plant_state,			
NULLIF(census_region, '.') AS census_region,			
NULLIF(nerc_region, '.') AS nerc_region,			
NULLIF(naics_code, '.') AS naics_code,			
NULLIF(sector_number, '.') AS sector_number,			
NULLIF(sector_name, '.') AS sector_name,			
NULLIF(generator_id, '.') AS generator_id,			
NULLIF(reported_prime_mover, '.') AS reported_prime_mover,
NULLIF(year, '.') AS year,
NULLIF(net_generation, '.') AS net_generation,
{{extract_month('months')}} as month

from {{source('staging','generator')}}
unpivot(net_generation for months in (	
net_generation_january,			
net_generation_february,			
net_generation_march,			
net_generation_april,			
net_generation_may,			
net_generation_june,			
net_generation_july,			
net_generation_august,			
net_generation_september,			
net_generation_october,			
net_generation_november,			
net_generation_december	))



{% if var('is_test_run', default=true) %}

limit 20

{% endif %}