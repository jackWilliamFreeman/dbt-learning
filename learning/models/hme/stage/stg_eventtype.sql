{{ config(materialized='ephemeral') }}

select *
from {{ source('STG_HME', 'EVENTTYPE') }}
where record_active_flag = 'Y' and isdeleted = 0