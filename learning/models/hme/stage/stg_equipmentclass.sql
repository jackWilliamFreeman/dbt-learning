{{ config(materialized='ephemeral') }}

select 
    *
from {{ source('STG_HME', 'EQUIPMENTCLASS') }}
where record_active_flag= 'Y'