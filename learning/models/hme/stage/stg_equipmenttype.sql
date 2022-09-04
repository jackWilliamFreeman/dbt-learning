{{ config(materialized='ephemeral') }}

select *
from {{ source('STG_HME', 'EQUIPMENTTYPE') }}
where record_active_flag = 'Y'