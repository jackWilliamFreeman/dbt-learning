{{ config(database = "SBX_AA_OPERATIONS_MANAGEMENT") }}

SELECT 
	ee.ID EVENT_ID,
	CYCLEID CYCLEID,
	sd.SHIFT_DATE,
	sd.SHIFT_YEAR,
	sd.SHIFT_MONTH,
	sd.SHIFT,
	ss.SOURCESYSTEMNAME AS SOURCE_SYSTEM_NAME,
	e.EQUIPMENTNAME  SOURCE_EQUIPMENT_NAME,
	e.REPORTEQUIPMENTNAME REPORTING_EQUIPMENT_NAME,
	ec.EQUIPMENTCLASSNAME  SOURCE_EQUIPMENT_CLASS_NAME,
	ec.REPORTEQUIPMENTCLASSNAME REPORTING_EQUIPMENT_CLASS_NAME,
	eqt.EQUIPMENTTYPENAME  SOURCE_EQUIPMENT_TYPE_NAME,
	eqt.REPORTEQUIPMENTTYPENAME REPORTING_EQUIPMENT_TYPE_NAME,
	sset.EventTypeName AS  SOURCE_EVENT_NAME,
	et.EventTypeName AS ASSIGNED_TUM_EVENT,
	teml.L1,
	teml.L2,
	teml.L3,
	teml.L4,
	teml.L5,
	teml.L6,
	teml.LOWEST_LEVEL_NAME,
	iff(tuv.VERSIONNAME IS NULL, 1, 0) IS_UNCLASSIFIED,
	teml.IS_CALENDAR_TIME,
	teml.IS_AVAILABLE_TIME,
	teml.IS_DOWNTIME,
	teml.IS_UTILISED_TIME,
	teml.IS_UNSCHEDULED_MAINTENANCE,
	teml.IS_SCHEDULED_MAINTENANCE,
	teml.IS_STANDBY_TIME,
	teml.IS_PRODUCTIVE_TIME,
	teml.IS_OPERATING_STANDBY,
	teml.IS_EFFECTIVE_TIME,
	teml.IS_NON_EFFECTIVE_TIME,
	teml.IS_BREAKDOWN,
	teml.IS_ACCIDENT_DAMAGE,
	teml.IS_SCHED_SERVICE_BACKLOG,
	teml.IS_SURPLUS_UNAVAILABLE,
	teml.IS_MAJOR_SCHED_SERVICE_ONSITE,
	teml.IS_MAJOR_SCHED_SERVICE_OFFSITE,
	teml.IS_OPERATING_DELAY,
	iff(tuv.VERSIONNAME IS NULL, DURATION_SECONDS, 0) UNCLASSIFIED_TIME,
	teml.IS_CALENDAR_TIME * DURATION_SECONDS CALENDAR_TIME ,
	teml.IS_AVAILABLE_TIME * DURATION_SECONDS AVAILABLE_TIME ,
	teml.IS_DOWNTIME * DURATION_SECONDS DOWNTIME ,
	teml.IS_UTILISED_TIME * DURATION_SECONDS UTILISED_TIME ,
	teml.IS_UNSCHEDULED_MAINTENANCE * DURATION_SECONDS UNSCHEDULED_MAINTENANCE ,
	teml.IS_SCHEDULED_MAINTENANCE * DURATION_SECONDS SCHEDULED_MAINTENANCE ,
	teml.IS_STANDBY_TIME * DURATION_SECONDS STANDBY_TIME ,
	teml.IS_PRODUCTIVE_TIME * DURATION_SECONDS PRODUCTIVE_TIME ,
	teml.IS_OPERATING_STANDBY * DURATION_SECONDS OPERATING_STANDBY ,
	teml.IS_EFFECTIVE_TIME * DURATION_SECONDS EFFECTIVE_TIME ,
	teml.IS_NON_EFFECTIVE_TIME * DURATION_SECONDS NON_EFFECTIVE_TIME ,
	teml.IS_BREAKDOWN * DURATION_SECONDS BREAKDOWN_TIME,
	teml.IS_ACCIDENT_DAMAGE * DURATION_SECONDS ACCIDENT_DAMAGE_TIME,
	teml.IS_SCHED_SERVICE_BACKLOG * DURATION_SECONDS SCHEDULED_SERVICE_BACKLOG_TIME,
	teml.IS_SURPLUS_UNAVAILABLE * DURATION_SECONDS SURPLUS_UNAVAILABLE_TIME,
	teml.IS_MAJOR_SCHED_SERVICE_ONSITE * DURATION_SECONDS MAJOR_SCHEDULED_SERVICE_ONSITE_TIME,
	teml.IS_MAJOR_SCHED_SERVICE_OFFSITE * DURATION_SECONDS MAJOR_SCHEDULED_SERVICE_OFFSITE_TIME,
	teml.IS_OPERATING_DELAY * DURATION_SECONDS OPERATING_DELAY_TIME,
	eebs.DURATION_SECONDS AS DURATION_SECONDS,
	RAW_EVENT_START_DATE_TIME,
	RAW_EVENT_END_DATE_TIME,
	EVENT_START_DATE_TIME,
	EVENT_END_DATE_TIME,
	o.OperationName OPERATION_NAME,
	tuv.VersionName AS TUM_VERSION_NAME
FROM
	{{ref('stg_equipmentevents')}} ee
INNER JOIN {{ref('int_tum_events_by_shift')}} eebs ON
	ee.id = eebs.EVENT_ID
INNER JOIN {{ref('stg_sourcesystemeventtype')}} sset ON
	sset.Id = ee.SourceSystemEventTypeId
INNER JOIN {{ref('stg_sourcesystem')}} ss ON
	ss.Id = ee.SourceSystemId
INNER JOIN {{ref('stg_equipment')}} e ON
	e.Id = ee.EquipmentId
LEFT OUTER JOIN {{ref('stg_equipmentclass')}} ec ON
	ec.Id = e.EquipmentClassId
LEFT OUTER JOIN {{ref('stg_equipmenttype')}} eqt ON
	eqt.ID = e.EQUIPMENTTYPEID
INNER JOIN {{ref('stg_operation')}} o ON
	o.Id = ee.OperationId
LEFT OUTER JOIN {{ref('stg_eventmapping')}} em ON
	sset.Id = em.SourceSystemEventTypeId
LEFT OUTER JOIN {{ref('stg_eventtype')}} et ON
	et.Id = em.EventTypeId
LEFT OUTER JOIN {{ref('stg_tumeventtypemapping')}} tetm ON
	tetm.EventTypeId = et.Id
LEFT OUTER JOIN {{ref('stg_timeusagelevel')}} tul ON 
	tul.Id = tetm.TimeUsageLevelId
LEFT OUTER JOIN {{ref('stg_timeusageversion')}} tuv ON 
	tuv.Id = tul.TimeUsageVersionId
	AND tuv.IsPublished = TRUE
	AND ee.StartDateTime >= tuv.EffectiveFrom
	AND ee.StartDateTime < COALESCE(tuv.EffectiveTo,
	'2999-01-01')
LEFT OUTER JOIN {{ref('int_tum_levels')}} teml ON 
		teml.EventTypeId = tetm.EventTypeId
	AND teml.TimeUsageVersionId = tuv.Id
LEFT OUTER JOIN {{source('COMMONTRANSFORM', 'SHIFT_DATES')}} sd
	-- Join Changed 22/7 as per request from Leo regarding performance.
	ON ee.ENDDATETIME  BETWEEN sd.START_TIME AND sd.END_TIME AND sd.shift_date = dateadd(hour, -6, ee.enddatetime)::DATE
WHERE (tuv.VERSIONNAME IS NOT NULL OR et.EventTypeName IS NULL)