{{config(materialized='view')}}


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
CAST(NULLIF(quantity, '.') AS FLOAT64) AS oil_stock_quantity,
{{extract_month('months')}} as month

FROM {{source('staging','oil_stocks')}}
unpivot(quantity for months in (	
quantity_january,			
quantity_february,			
quantity_march,			
quantity_april,			
quantity_may,			
quantity_june,			
quantity_july,			
quantity_august,			
quantity_september,			
quantity_october,			
quantity_november,			
quantity_december	))



{% if var('is_test_run', default=true) %}

limit 20

{% endif %}