{{ config(materialized="incremental") }}

-- Load the stocks table



SELECT * FROM {{ref("stg_stocks")}}

{% if is_incremental() %}

where year > (SELECT max(year) FROM {{ this }})

{% endif %}

{% if var('is_test_run', default=true) %}

limit 20

{% endif %}