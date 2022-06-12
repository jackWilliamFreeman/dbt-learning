select event_id,
        startdatetime,
        enddatetime,
        equipmentname,
        reportequipmentname
from {{ ref('stg_equipmentevents') }} as equipmentevents
inner join {{ ref('stg_equipment') }} as equipment on equipment.equipment_id = equipmentevents.equipmentid