{{ config(materialized="table") }}

-- Create dimenstion tavle for the plants details
select
    plant_id,
    plant_state,
    sector_number,
    naics_code,
    plant_name,
    combined_heat_and_power_status
from {{ ref("stg_plant_frame") }}
group by
    plant_id,
    plant_state,
    sector_number,
    naics_code,
    plant_name,
    combined_heat_and_power_status

{% if var('is_test_run', default=true) %}

limit 20

{% endif %}
