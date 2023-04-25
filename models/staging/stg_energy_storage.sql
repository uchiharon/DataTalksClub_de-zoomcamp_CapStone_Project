{{config(materialized='view')}}

-- Load the energy storage table
with energy_storage_data as (
    select *, row_number() over(order by cast(plant_id as int)) as row_number from {{source('staging','energy_storage')}}
),

-- Create a table for the quantity of energy stored
-- This was achieved by unpivotting the data columns

quantity_of_energy_stored_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
plant_name,			
operator_name,			
operator_id,			
plant_state,			
census_region,			
nerc_region,			
naics_code,			
eia_sector_number,			
sector_name,			
reported_prime_mover,			
reported_fuel_type_code,			
aer_fuel_type_code,			
physical_unit_label,
year,
quantity_of_energy_stored,
{{extract_month('months')}} as month

from energy_storage_data 
unpivot(quantity_of_energy_stored for months in (quantity_january,			
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
quantity_december))),



-- Create a table for the electricity quantity stored
-- This was achieved by unpivotting the data columns

electricity_quantity_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
plant_name,			
operator_name,			
operator_id,			
plant_state,			
census_region,			
nerc_region,			
naics_code,			
eia_sector_number,			
sector_name,			
reported_prime_mover,			
reported_fuel_type_code,			
aer_fuel_type_code,			
physical_unit_label,
year,
elec_quantity,
{{extract_month('months')}} as month

from energy_storage_data 
unpivot(elec_quantity for months in (elec_quantity_january,			
elec_quantity_february,			
elec_quantity_march,			
elec_quantity_april,			
elec_quantity_may,			
elec_quantity_june,			
elec_quantity_july,			
elec_quantity_august,			
elec_quantity_september,			
elec_quantity_october,			
elec_quantity_november,			
elec_quantity_december))),


-- Create a table for the gross electricity generated
-- This was achieved by unpivotting the data columns

gross_generation_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
plant_name,			
operator_name,			
operator_id,			
plant_state,			
census_region,			
nerc_region,			
naics_code,			
eia_sector_number,			
sector_name,			
reported_prime_mover,			
reported_fuel_type_code,			
aer_fuel_type_code,			
physical_unit_label,
year,
gross_gen,
{{extract_month('months')}} as month

from energy_storage_data 
unpivot(gross_gen for months in (grossgen_january,			
grossgen_february,			
grossgen_march,			
grossgen_april,			
grossgen_may,			
grossgen_june,			
grossgen_july,			
grossgen_august,			
grossgen_september,			
grossgen_october,			
grossgen_november,			
grossgen_december))),


-- Create a table for the net electricity generated
-- This was achieved by unpivotting the data columns

net_generation_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
plant_name,			
operator_name,			
operator_id,			
plant_state,			
census_region,			
nerc_region,			
naics_code,			
eia_sector_number,			
sector_name,			
reported_prime_mover,			
reported_fuel_type_code,			
aer_fuel_type_code,			
physical_unit_label,
year,
net_gen,
{{extract_month('months')}} as month

from energy_storage_data 
unpivot(net_gen for months in (netgen_january,			
netgen_february,			
netgen_march,			
netgen_april,			
netgen_may,			
netgen_june,			
netgen_july,			
netgen_august,			
netgen_september,			
netgen_october,			
netgen_november,			
netgen_december))),

-- Join unpivotted tables
energy_and_elec_data as (
    select qt.*, eq.elec_quantity from quantity_of_energy_stored_data qt join electricity_quantity_data eq
    on qt.row_number = eq.row_number and qt.year = eq.year and qt.month = eq.month 
),

gross_and_net_generation_data as (
    select gg.*, ng.net_gen from gross_generation_data gg join net_generation_data ng
    on gg.row_number = ng.row_number and gg.year = ng.year and gg.month = ng.month 
),

-- Finalise joining

semi_transformed_energy_storage_data as (select ee.*, gn.gross_gen, gn.net_gen from energy_and_elec_data ee join gross_and_net_generation_data gn
on ee.row_number = ee.row_number and ee.year = ee.year and ee.month = ee.month)

SELECT NULLIF( plant_id, '.') AS plant_id,			
NULLIF( combined_heat_and_power_plant, '.') AS combined_heat_and_power_plant,			
NULLIF( plant_name, '.') AS plant_name,			
NULLIF( operator_name, '.') AS operator_name,			
NULLIF( operator_id, '.') AS operator_id,			
NULLIF( plant_state, '.') AS plant_state,			
NULLIF( census_region, '.') AS census_region,			
NULLIF( nerc_region, '.') AS nerc_region,			
NULLIF( naics_code, '.') AS naics_code,			
NULLIF( eia_sector_number, '.') AS eia_sector_number,			
NULLIF( sector_name, '.') AS sector_name,			
NULLIF( reported_prime_mover, '.') AS reported_prime_mover,			
NULLIF( reported_fuel_type_code, '.') AS reported_fuel_type_code,			
NULLIF( aer_fuel_type_code, '.') AS aer_fuel_type_code,			
NULLIF( physical_unit_label, '.') AS physical_unit_label,
CAST(NULLIF( quantity_of_energy_stored, '.') AS FLOAT64) AS quantity_of_energy_stored,
CAST(NULLIF( elec_quantity, '.') AS FLOAT64) AS elec_quantity,
CAST(NULLIF( gross_gen, '.') AS FLOAT64) AS gross_gen,
CAST(NULLIF( net_gen, '.') AS FLOAT64) AS net_gen,
NULLIF( month, '.') AS month,
NULLIF( year, '.') AS year

FROM semi_transformed_energy_storage_data

{% if var('is_test_run', default=true) %}

limit 20

{% endif %}


