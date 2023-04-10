{{config(materialized='view')}}

-- Load the petcoke stocks table

SELECT 
NULLIF(plant_id, '.') AS plant_id,			
NULLIF(combined_heat_and_power_plant, '.') AS combined_heat_and_power_plant,			
NULLIF(plant_name, '.') AS plant_name,			
NULLIF(operator_name, '.') AS operator_name,			
NULLIF(operator_id, '.') AS operator_id,			
NULLIF(plant_state, '.') AS plant_state,			
NULLIF(census_region, '.') AS census_region,			
NULLIF(nerc_region, '.') AS nerc_region,			
NULLIF(naics_code, '.') AS naics_code,			
NULLIF(eia_sector_number, '.') AS eia_sector_number,			
NULLIF(sector_name, '.') AS sector_name,			
NULLIF(reported_fuel_type_code, '.') AS reported_fuel_type_code,			
NULLIF(aer_fuel_type_code, '.') AS aer_fuel_type_code,			
NULLIF(physical_unit_label, '.') AS physical_unit_label,
NULLIF(year, '.') AS year,
NULLIF(quantity, '.') AS quantity,
{{extract_month('months')}} as month


from {{source('staging','petcoke_stocks')}}
unpivot(quantity for months in (	
NULLIF(quantity_january, '.') AS quantity_january,			
NULLIF(quantity_february, '.') AS quantity_february,			
NULLIF(quantity_march, '.') AS quantity_march,			
NULLIF(quantity_april, '.') AS quantity_april,			
NULLIF(quantity_may, '.') AS quantity_may,			
NULLIF(quantity_june, '.') AS quantity_june,			
NULLIF(quantity_july, '.') AS quantity_july,			
NULLIF(quantity_august, '.') AS quantity_august,			
NULLIF(quantity_september, '.') AS quantity_september,			
NULLIF(quantity_october, '.') AS quantity_october,			
NULLIF(quantity_november, '.') AS quantity_november,			
NULLIF(quantity_december, '.') AS quantity_december	))



{% if var('is_test_run', default=true) %}

limit 20

{% endif %}