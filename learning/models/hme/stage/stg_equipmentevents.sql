{{ config(materialized='ephemeral') }}

select id as event_id,
       equipmentid,
       startdatetime,
       enddatetime
from {{ source('STG_HME', 'EQUIPMENTEVENT') }}
where record_active_flag = 'Y'