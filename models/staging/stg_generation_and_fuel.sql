{{config(materialized='view')}}

-- Load the generation and fuel table
with generation_and_fuel as (
    select *, row_number() over(order by cast(plant_id as int)) as row_number from {{source('staging','generation_and_fuel')}}
),


-- Create a table for the quantity of energy generated
-- This was achieved by unpivotting the data columns

quantity_gen_data as (SELECT row_number,
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
{{extract_month('months')}} as month

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
))),



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
{{extract_month('months')}} as month

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
))),



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
{{extract_month('months')}} as month

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
))),



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
{{extract_month('months')}} as month

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
))),




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
{{extract_month('months')}} as month

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
))),


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
{{extract_month('months')}} as month

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
))),




quan_and_elec_quan_data as (
    select qt.*, eq.elec_quantity from quantity_gen_data qt join elec_quantity_gen_data eq
    on qt.row_number = eq.row_number and qt.year = eq.year and qt.month = eq.month 
),

mmbtuper_unit_and_tot_mmbtu_data as (
    select qt.*, eq.tot_mmbtu from mmbtuper_unit_gen_data qt join tot_mmbtu_gen_data eq
    on qt.row_number = eq.row_number and qt.year = eq.year and qt.month = eq.month 
),

elec_mmbtu_and_netgen_data as (
    select qt.*, eq.netgen from elec_mmbtu_gen_data qt join netgen_gen_data eq
    on qt.row_number = eq.row_number and qt.year = eq.year and qt.month = eq.month 
),

semi_transformed_gen_data as (
    SELECT qt.*, eq.mmbtuper_unit, eq.tot_mmbtu,  en.elec_mmbtu, en.netgen 
    FROM quan_and_elec_quan_data qt
    JOIN mmbtuper_unit_and_tot_mmbtu_data eq
    on qt.row_number = eq.row_number and qt.year = eq.year and qt.month = eq.month 
    JOIN elec_mmbtu_and_netgen_data en
    on qt.row_number = en.row_number and qt.year = en.year and qt.month = en.month 

)

SELECT
NULLIF(plant_id,'.') AS plant_id,			
NULLIF(combined_heat_and_power_plant,'.') AS combined_heat_and_power_plant,			
NULLIF(nuclear_unit_id,'.') AS nuclear_unit_id,			
NULLIF(plant_name,'.') AS plant_name,			
NULLIF(operator_name,'.') AS operator_name,			
NULLIF(operator_id,'.') AS operator_id,			
NULLIF(plant_state,'.') AS plant_state,			
NULLIF(census_region,'.') AS census_region,			
NULLIF(nerc_region,'.') AS nerc_region,			
NULLIF(naics_code,'.') AS naics_code,			
NULLIF(eia_sector_number,'.') AS eia_sector_number,			
NULLIF(sector_name,'.') AS sector_name,			
NULLIF(reported_prime_mover,'.') AS reported_prime_mover,			
NULLIF(reported_fuel_type_code,'.') AS reported_fuel_type_code,			
NULLIF(aer_fuel_type_code,'.') AS aer_fuel_type_code,			
NULLIF(physical_unit_label,'.') AS physical_unit_label,
CAST(NULLIF(quantity,'.') AS FLOAT64) AS quantity,
CAST(NULLIF(elec_quantity,'.') AS FLOAT64) AS elec_quantity,
CAST(NULLIF(mmbtuper_unit,'.') AS FLOAT64) AS mmbtuper_unit,
CAST(NULLIF(tot_mmbtu,'.') AS FLOAT64) AS tot_mmbtu,
CAST(NULLIF(elec_mmbtu,'.') AS FLOAT64) AS elec_mmbtu,
CAST(NULLIF(netgen,'.') AS FLOAT64) AS netgen,
NULLIF(year,'.') AS year,
NULLIF(month,'.') AS month

FROM semi_transformed_gen_data


{% if var('is_test_run', default=true) %}

limit 20

{% endif %}