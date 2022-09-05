{{ config(materialized='table', database = "AA_OPERATIONS_MANAGEMENT", schema = 'SLN_HME') }}

SELECT
	OPERATION_NAME,
	e.CYCLEID,
	MAX(e.equipment_Id) as EQUIPMENT_ID,
	SUM(UNCLASSIFIED_TIME) as SUM_UNCLASSIFIED_TIME,
	SUM(CALENDAR_TIME) SUM_CALENDAR_TIME,
	SUM(AVAILABLE_TIME) SUM_AVAILABLE_TIME,
	SUM(DOWNTIME) SUM_DOWNTIME,
	SUM(UTILISED_TIME) SUM_UTILISED_TIME,
	SUM(UNSCHEDULED_MAINTENANCE) SUM_UNSCHEDULED_MAINTENANCE,
	SUM(SCHEDULED_MAINTENANCE) SUM_SCHEDULED_MAINTENANCE,
	SUM(STANDBY_TIME) SUM_STANDBY_TIME,
	SUM(PRODUCTIVE_TIME) SUM_PRODUCTIVE_TIME,
	SUM(OPERATING_STANDBY) SUM_OPERATING_STANDBY,
	SUM(EFFECTIVE_TIME) SUM_EFFECTIVE_TIME,
	SUM(NON_EFFECTIVE_TIME) SUM_NON_EFFECTIVE_TIME,
	SUM(BREAKDOWN) SUM_BREAKDOWN,
	SUM(ACCIDENT_DAMAGE) SUM_ACCIDENT_DAMAGE,
	SUM(SCHED_SERVICE_BACKLOG) SUM_SCHED_SERVICE_BACKLOG,
	SUM(SURPLUS_UNAVAILABLE) SUM_SURPLUS_UNAVAILABLE,
	SUM(MAJOR_SCHED_SERVICE_ONSITE) SUM_MAJOR_SCHED_SERVICE_ONSITE,
	SUM(MAJOR_SCHED_SERVICE_OFFSITE) SUM_MAJOR_SCHED_SERVICE_OFFSITE,
	SUM(DURATION_SECONDS) SUM_DURATION_SECONDS,
	max(EVENT_MODIFIED_DT) RECORD_MODIFIED_DT
FROM
	{{ref('hme_tum_events')}} e
GROUP BY
	OPERATION_NAME,
	e.CYCLEID