{{config(materialized='table')}}

-- Load the stocks table



SELECT * FROM {{ref("stg_stocks")}}

{% if var('is_test_run', default=true) %}

limit 20

{% endif %}