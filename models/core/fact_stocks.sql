{{ config(materialized="incremental") }}


WITH stock as (SELECT 

plant_id,								
operator_name,			
operator_id,						
census_region,			
nerc_region,					
eia_sector_number,					
reported_fuel_type_code,			
aer_fuel_type_code,			
physical_unit_label,			
null as coal_stock_quantity,
oil_stock_quantity,	
null as petcoke_stock_quantity,	
year,			
month,	




FROM {{ref("stg_oil_stocks")}} 

UNION ALL 


SELECT 

plant_id,				
operator_name,			
operator_id,						
census_region,			
nerc_region,					
eia_sector_number,						
reported_fuel_type_code,			
aer_fuel_type_code,			
physical_unit_label,			
null as coal_stock_quantity,	
null as oil_stock_quantity,
petcoke_stock_quantity,	
year,	
month


FROM {{ref("stg_petcoke_stocks")}}


UNION ALL 


SELECT 

plant_id,				
operator_name,			
operator_id,						
census_region,			
nerc_region,					
eia_sector_number,						
reported_fuel_type_code,			
aer_fuel_type_code,			
physical_unit_label,			
coal_stock_quantity,	
null as oil_stock_quantity,		
null as petcoke_stock_quantity,	
year,	
month


FROM {{ref("stg_coal_stocks")}}
)


SELECT

plant_id,				
operator_name,			
operator_id,						
census_region,			
nerc_region,										
reported_fuel_type_code,			
aer_fuel_type_code,			
physical_unit_label,			
IFNULL(coal_stock_quantity,0) as coal_stock_quantity,	
IFNULL(oil_stock_quantity,0) as oil_stock_quantity,		
IFNULL(petcoke_stock_quantity,0) as petcoke_stock_quantity,	
year,	
month

FROM stock

{% if is_incremental() %}

where year > (SELECT max(year) FROM {{ this }})

{% endif %}

{% if var('is_test_run', default=true) %}

limit 20

{% endif %}