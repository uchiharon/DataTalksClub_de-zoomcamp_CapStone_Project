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
{{extract_month('months')}} as month

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
{{extract_month('months')}} as month

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
{{extract_month('months')}} as month


FROM boiler_fuel_data 
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
{{extract_month('months')}} as month


FROM boiler_fuel_data 
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
ash_content_december))),


-- Join unpivotted tables
quantity_and_heat_data as (
    select qt.*, hc.heat_content_per_unit from quantity_of_fuel_consumed_data qt join heat_content_per_unit_data hc
    on qt.row_number = hc.row_number and qt.year = hc.year and qt.month = hc.month 
),

sulfur_and_ash_data as (
    select sc.*, ac.ash_content from sulfur_content_data sc join ash_content_data ac
    on sc.row_number = ac.row_number and sc.year = ac.year and sc.month = ac.month 
),

-- Finalise joining

semi_transformed_boiler_fuel_data as (select qh.*, sa.sulfur_content, sa.ash_content from quantity_and_heat_data qh join sulfur_and_ash_data sa
on qh.row_number = sa.row_number and qh.year = sa.year and qh.month = sa.month)


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
    NULLIF(boiler_id, '.') AS boiler_id,
    NULLIF(reported_prime_mover, '.') AS reported_prime_mover,
    NULLIF(reported_fuel_type_code, '.') AS reported_fuel_type_code,
    NULLIF(physical_unit_label, '.') AS physical_unit_label,
    -- measured values
    CAST(NULLIF(quantity_of_fuel_consumed, '.') AS FLOAT64) AS quantity_of_boiler_fuel,
    CAST(NULLIF(heat_content_per_unit, '.') AS FLOAT64) AS heat_content_per_unit,
    CAST(NULLIF(sulfur_content, '.') AS FLOAT64) AS sulfur_content,
    CAST(NULLIF(ash_content, '.') AS FLOAT64) AS ash_content,
    NULLIF(year, '.') AS year,
    NULLIF(month, '.') AS month

FROM semi_transformed_boiler_fuel_data



{% if var('is_test_run', default=true) %}

limit 20

{% endif %}