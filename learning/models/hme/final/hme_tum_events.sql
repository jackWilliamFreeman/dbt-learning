{{ config(database = "AA_OPERATIONS_MANAGEMENT") }}

SELECT
	ee.ID,
	ee.CYCLEID,
	sd.SHIFT_DATE,
	sd.SHIFT_YEAR,
	sd.SHIFT_MONTH,
	sd.SHIFT,
	ss.SourceSystemName AS SOURCE_SYSTEM_NAME,
	e.EquipmentName AS SOURCE_EQUIPMENT_NAME,
	e.ReportEquipmentName AS REPORTING_EQUIPMENT_NAME,
	ec.EquipmentClassName AS SOURCE_EQUIPMENT_CLASS_NAME,
	ec.ReportEquipmentClassName AS REPORTING_EQUIPMENT_CLASS_NAME,
	eqt.EQUIPMENTTYPENAME AS SOURCE_EQUIPMENT_TYPE_NAME,
	eqt.REPORTEQUIPMENTTYPENAME AS REPORTING_EQUIPMENT_TYPE_NAME,
	sset.EventTypeName AS SOURCE_EVENT_NAME,
	et.EventTypeName AS ASSIGNED_TUM_EVENT,
	teml.L1,
	teml.L2,
	teml.L3,
	teml.L4,
	teml.L5,
	teml.L6,
	teml.LOWEST_LEVEL_NAME,
	iff(tuv.VERSIONNAME IS NULL,
	1,
	0) IS_UNCLASSIFIED,
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
	iff(tuv.VERSIONNAME IS NULL,
	1,
	0) * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) UNCLASSIFIED_TIME,
	teml.IS_CALENDAR_TIME * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) CALENDAR_TIME,
	teml.IS_AVAILABLE_TIME * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) AVAILABLE_TIME,
	teml.IS_DOWNTIME * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) DOWNTIME,
	teml.IS_UTILISED_TIME * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) UTILISED_TIME,
	teml.IS_UNSCHEDULED_MAINTENANCE * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) UNSCHEDULED_MAINTENANCE,
	teml.IS_SCHEDULED_MAINTENANCE * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) SCHEDULED_MAINTENANCE,
	teml.IS_STANDBY_TIME * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) STANDBY_TIME,
	teml.IS_PRODUCTIVE_TIME * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) PRODUCTIVE_TIME,
	teml.IS_OPERATING_STANDBY * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) OPERATING_STANDBY,
	teml.IS_OPERATING_DELAY * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) OPERATING_DELAY_TIME,
	teml.IS_EFFECTIVE_TIME * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) EFFECTIVE_TIME,
	teml.IS_NON_EFFECTIVE_TIME * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) NON_EFFECTIVE_TIME,
	teml.IS_BREAKDOWN * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) BREAKDOWN,
	teml.IS_ACCIDENT_DAMAGE * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) ACCIDENT_DAMAGE,
	teml.IS_SCHED_SERVICE_BACKLOG * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) SCHED_SERVICE_BACKLOG,
	teml.IS_SURPLUS_UNAVAILABLE * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) SURPLUS_UNAVAILABLE,
	teml.IS_MAJOR_SCHED_SERVICE_ONSITE * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) MAJOR_SCHED_SERVICE_ONSITE,
	teml.IS_MAJOR_SCHED_SERVICE_OFFSITE * datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) MAJOR_SCHED_SERVICE_OFFSITE,
	datediff(seconds,
	ee.StartDateTime,
	COALESCE(ee.EndDateTime,
	current_timestamp())) AS DURATION_SECONDS,
	ee.StartDateTime AS EVENT_START_DATE_TIME,
	ee.EndDateTime AS EVENT_END_DATE_TIME,
	o.OperationName AS OPERATION_NAME,
	tuv.VersionName AS TUM_VERSION_NAME,
	ee.RECORD_MODIFIED_DT AS EVENT_MODIFIED_DT,
	e.RECORD_MODIFIED_DT AS EQUIPMENT_MODIFIED_DT,
	ec.RECORD_MODIFIED_DT AS EQUIPMENT_CLASS_MODIFIED_DT,
	et.RECORD_MODIFIED_DT AS EQUIPMENT_TYPE_MODIFIED_DT,
	e.id as EQUIPMENT_ID
FROM
	{{ref('stg_equipmentevents')}} ee
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
LEFT OUTER JOIN {{ref('stg_commontransform_shiftdates')}} sd
	-- Join Changed 22/7 as per request from Leo regarding performance.
	ON ee.ENDDATETIME  BETWEEN sd.START_TIME AND sd.END_TIME AND sd.shift_date = dateadd(hour, -6, ee.enddatetime)::DATE
WHERE (tuv.VERSIONNAME IS NOT NULL OR et.EventTypeName IS NULL)