{{config(materialized='view')}}

-- Load the energy storage table
with generation_and_fuel as (
    select *, row_number() over(order by cast(plant_id as int)) as row_number from {{source('staging','generation_and_fuel')}}
),


-- Create a table for the quantity of energy generated
-- This was achieved by unpivotting the data columns

quantity_of_energy_gen_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
nuclear_unit_id,			
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
quantity,
months

FROM generation_and_fuel
UNPIVOT(quantity for months in (
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
    quantity_december
)))



-- Create a table for the electricity quantity generated
-- This was achieved by unpivotting the data columns

elec_quantity_gen_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
nuclear_unit_id,			
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
months

FROM generation_and_fuel
UNPIVOT(elec_quantity for months in (
    elec_quantity_january,			
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
    elec_quantity_december
)))



-- Create a table for the mmbtuper_unit generated
-- This was achieved by unpivotting the data columns

mmbtuper_unit_gen_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
nuclear_unit_id,			
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
mmbtuper_unit,
months

FROM generation_and_fuel
UNPIVOT(mmbtuper_unit for months in (
    mmbtuper_unit_january,			
    mmbtuper_unit_february,			
    mmbtuper_unit_march,			
    mmbtuper_unit_april,			
    mmbtuper_unit_may,			
    mmbtuper_unit_june,			
    mmbtuper_unit_july,			
    mmbtuper_unit_august,			
    mmbtuper_unit_september,			
    mmbtuper_unit_october,			
    mmbtuper_unit_november,			
    mmbtuper_unit_december
)))



-- Create a table for the tot_mmbtu generated
-- This was achieved by unpivotting the data columns

tot_mmbtu_gen_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
nuclear_unit_id,			
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
tot_mmbtu,
months

FROM generation_and_fuel
UNPIVOT(tot_mmbtu for months in (
    tot_mmbtu_january,			
    tot_mmbtu_february,			
    tot_mmbtu_march,			
    tot_mmbtu_april,			
    tot_mmbtu_may,			
    tot_mmbtu_june,			
    tot_mmbtu_july,			
    tot_mmbtu_august,			
    tot_mmbtu_september,			
    tot_mmbtu_october,			
    tot_mmbtu_november,			
    tot_mmbtu_december
)))




-- Create a table for the elec_mmbtu generated
-- This was achieved by unpivotting the data columns

elec_mmbtu_gen_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
nuclear_unit_id,			
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
elec_mmbtu,
months

FROM generation_and_fuel
UNPIVOT(elec_mmbtu for months in (
    elec_mmbtu_january,			
    elec_mmbtu_february,			
    elec_mmbtu_march,			
    elec_mmbtu_april,			
    elec_mmbtu_may,			
    elec_mmbtu_june,			
    elec_mmbtu_july,			
    elec_mmbtu_august,			
    elec_mmbtu_september,			
    elec_mmbtu_october,			
    elec_mmbtu_november,			
    elec_mmbtu_december	
)))


-- Create a table for the netgen generated
-- This was achieved by unpivotting the data columns

netgen_gen_data as (SELECT row_number,
plant_id,			
combined_heat_and_power_plant,			
nuclear_unit_id,			
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
netgen,
months

FROM generation_and_fuel
UNPIVOT(netgen for months in (
    netgen_january,			
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
    netgen_december
)))