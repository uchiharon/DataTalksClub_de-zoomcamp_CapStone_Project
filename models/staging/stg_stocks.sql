{{config(materialized='view')}}

-- Load the energy storage table
with stock_data as (
    select *, row_number() over(order by census_division_and_state) as row_number from {{source('staging','stocks')}}
),

-- Create a table for the coal
-- This was achieved by unpivotting the data columns

coal_data as (SELECT row_number,
NULLIF(census_division_and_state, '.') AS census_division_and_state,
coal,
year,
{{extract_month('months')}} as month

FROM stock_data
UNPIVOT(coal for months in (
coal_january,			
coal_february,			
coal_march,			
coal_april,			
coal_may,			
coal_june,			
coal_july,			
coal_august,			
coal_september,			
coal_october,			
coal_november,			
coal_december
))),


-- Create a table for the oil
-- This was achieved by unpivotting the data columns

oil_data as (SELECT row_number,
NULLIF(census_division_and_state, '.') AS census_division_and_state,
oil,
year,
{{extract_month('months')}} as month

FROM stock_data
UNPIVOT(oil for months in (
oil_january,			
oil_february,			
oil_march,			
oil_april,			
oil_may,			
oil_june,			
oil_july,			
oil_august,			
oil_september,			
oil_october,			
oil_november,			
oil_december
))),




-- Create a table for the petcoke
-- This was achieved by unpivotting the data columns

petcoke_data as (SELECT row_number,
NULLIF(census_division_and_state, '.') AS census_division_and_state,
petcoke,
year,
{{extract_month('months')}} as month

FROM stock_data
UNPIVOT(petcoke for months in (
petcoke_january,			
petcoke_february,			
petcoke_march,			
petcoke_april,			
petcoke_may,			
petcoke_june,			
petcoke_july,			
petcoke_august,			
petcoke_september,			
petcoke_october,			
petcoke_november,			
petcoke_december
))),



semi_transformed_data as (SELECT cd.*, od.oil, pd.petcoke  FROM coal_data as cd
JOIN oil_data as od
ON cd.row_number = od.row_number and cd.census_division_and_state = od.census_division_and_state  and cd.month = od.month
JOIN petcoke_data as pd
ON cd.row_number = pd.row_number and cd.census_division_and_state = pd.census_division_and_state  and cd.month = pd.month)


SELECT 
NULLIF(census_division_and_state,'.') AS census_division_and_state,
CAST(NULLIF(NULLIF(coal,'.'), 'W') AS FLOAT64) AS coal_stocks,
CAST(NULLIF(NULLIF(oil,'.'), 'W') AS FLOAT64) AS oil_stocks,
CAST(NULLIF(NULLIF(petcoke,'.'), 'W') AS FLOAT64) AS petcoke,
NULLIF(month,'.') AS month,
NULLIF( year, '.') AS year

FROM semi_transformed_data



{% if var('is_test_run', default=true) %}

limit 20

{% endif %}
