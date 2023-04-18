{{config(materialized='view')}}

-- Load the plant_frame table

	
SELECT NULLIF(year, '.') AS year,			
NULLIF(plant_id, '.') AS plant_id,			
NULLIF(plant_state, '.') AS plant_state,			
NULLIF(sector_number, '.') AS sector_number,			
NULLIF(naics_code, '.') AS naics_code,			
NULLIF(plant_name, '.') AS plant_name,			
NULLIF(combined_heat_and_power_status, '.') AS combined_heat_and_power_status

from {{source('staging','plant_frame')}}




{% if var('is_test_run', default=true) %}

limit 20

{% endif %}