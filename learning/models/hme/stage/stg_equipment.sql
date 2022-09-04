{{ config(materialized='ephemeral') }}

select 
    id as equipment_id,
    equipmentname,
    reportequipmentname
from {{ source('STG_HME', 'EQUIPMENT') }}
where record_active_flag= 'Y'