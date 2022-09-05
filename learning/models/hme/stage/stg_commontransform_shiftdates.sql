{{ config(materialized='ephemeral', database='AA_OPERATIONS_MANAGEMENT') }}

select *
from {{ source('COMMONTRANSFORM', 'SHIFT_DATES') }}