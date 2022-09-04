{{ config(materialized='ephemeral') }}

select 
    *
from {{ source('STG_HME', 'EQUIPMENT') }}
where record_active_flag= 'Y' 