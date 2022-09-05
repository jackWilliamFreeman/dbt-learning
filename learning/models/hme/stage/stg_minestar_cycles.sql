{{ config(materialized='ephemeral')}}

select *
from {{ source('MINESTAR', 'CYCLE') }}
where record_active_flag = 'Y'