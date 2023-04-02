{{config(materialized='view')}}

-- Load the boiler fuel table
with boiler_fuel_data as (
    select *, row_number() over(order by cast(plant_id as int)) as row_number from {{source('staging','boiler_fuel')}}
),


-- Create a table for the quantify of fuel consumed
-- This was achieved by unpivotting the data columns
quantity_of_fuel_consumed_data as (SELECT row_number,
plant_id,
combined_heat_and__power_plant,
plant_name,
operator_name,
operator_id,
plant_state,
census_region,
nerc_region,
naics_code,
sector_number,
sector_name,
boiler_id,
reported_prime_mover,
reported_fuel_type_code,
physical_unit_label,
year,
quantity_of_fuel_consumed,
months

FROM boiler_fuel_data 
unpivot(quantity_of_fuel_consumed for months in (quantity_of_fuel_consumed_january,
  quantity_of_fuel_consumed_february,
  quantity_of_fuel_consumed_march,
  quantity_of_fuel_consumed_april,
  quantity_of_fuel_consumed_may,
  quantity_of_fuel_consumed_june,
  quantity_of_fuel_consumed_july,
  quantity_of_fuel_consumed_august,
  quantity_of_fuel_consumed_september,
  quantity_of_fuel_consumed_october,
  quantity_of_fuel_consumed_november,
  quantity_of_fuel_consumed_december))),


-- Create a table for the heat content per unit
-- This was achieved by unpivotting the data columns

heat_content_per_unit_data as (SELECT row_number,
plant_id,
combined_heat_and__power_plant,
plant_name,
operator_name,
operator_id,
plant_state,
census_region,
nerc_region,
naics_code,
sector_number,
sector_name,
boiler_id,
reported_prime_mover,
reported_fuel_type_code,
physical_unit_label,
year,
heat_content_per_unit,
months

FROM boiler_fuel_data 
unpivot(heat_content_per_unit for months in (
mmbtu_per_unit_january,
mmbtu_per_unit_february,
mmbtu_per_unit_march,
mmbtu_per_unit_april,
mmbtu_per_unit_may,
mmbtu_per_unit_june,
mmbtu_per_unit_july,
mmbtu_per_unit_august,
mmbtu_per_unit_september,
mmbtu_per_unit_october,
mmbtu_per_unit_november,
mmbtu_per_unit_december))),





-- Create a table for the heat sulfur content
-- This was achieved by unpivotting the data columns

sulfur_content_data as (SELECT row_number,
plant_id,
combined_heat_and__power_plant,
plant_name,
operator_name,
operator_id,
plant_state,
census_region,
nerc_region,
naics_code,
sector_number,
sector_name,
boiler_id,
reported_prime_mover,
reported_fuel_type_code,
physical_unit_label,
year,
sulfur_content,
months


FROM test 
unpivot(sulfur_content for months in (
sulfur_content_january,
sulfur_content_february,
sulfur_content_march,
sulfur_content_april,
sulfur_content_may,
sulfur_content_june,
sulfur_content_july,
sulfur_content_august,
sulfur_content_september,
sulfur_content_october,
sulfur_content_november,
sulfur_content_december))),



-- Create a table for the heat ash content
-- This was achieved by unpivotting the data columns

ash_content_data as (SELECT row_number,
plant_id,
combined_heat_and__power_plant,
plant_name,
operator_name,
operator_id,
plant_state,
census_region,
nerc_region,
naics_code,
sector_number,
sector_name,
boiler_id,
reported_prime_mover,
reported_fuel_type_code,
physical_unit_label,
year,
ash_content,
months


FROM test 
unpivot(ash_content for months in (
ash_content_january,
ash_content_february,
ash_content_march,
ash_content_april,
ash_content_may,
ash_content_june,
ash_content_july,
ash_content_august,
ash_content_september,
ash_content_october,
ash_content_november,
ash_content_december)))






{% if var('is_test_run', default=true) %}

limit 20

{% endif %}