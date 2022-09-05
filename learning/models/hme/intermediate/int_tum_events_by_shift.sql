	{{ config(materialized='table', database = "AA_OPERATIONS_MANAGEMENT", schema = 'SLN_HME') }}

    /* Events spanning a shift - additional records */
	SELECT
	ee.ID EVENT_ID,
	sd.SHIFT_DATE,
	sd.SHIFT_YEAR,
	sd.SHIFT_MONTH,
	sd.SHIFT,
	ee.STARTDATETIME AS RAW_EVENT_START_DATE_TIME,
	greatest(ee.STARTDATETIME, SD.START_TIME) as EVENT_START_DATE_TIME,
	COALESCE(ee.ENDDATETIME,current_timestamp) AS RAW_EVENT_END_DATE_TIME,
	LEAST(COALESCE(ee.ENDDATETIME, current_timestamp), SD.PROCESSING_END_TIME) as EVENT_END_DATE_TIME,
	round(datediff(microseconds,greatest(ee.STARTDATETIME, SD.START_TIME), coalesce(EVENT_END_DATE_TIME, current_timestamp()))/1000000) DURATION_SECONDS

	FROM {{ref('stg_equipmentevents')}} ee
	     INNER JOIN {{ref('stg_commontransform_shiftdates')}} sd
	     	ON sd.PROCESSING_END_TIME > ee.STARTDATETIME  AND sd.PROCESSING_END_TIME < COALESCE(ee.ENDDATETIME, current_timestamp)
	union all
    /* Events not spanning a shift and the end for all events */
	SELECT
	ee.Id EVENT_ID,
	sd.SHIFT_DATE,
	sd.SHIFT_YEAR,
	sd.SHIFT_MONTH,
	sd.SHIFT,
	ee.STARTDATETIME AS RAW_EVENT_START_DATE_TIME,
	greatest(ee.STARTDATETIME, SD.START_TIME) as EVENT_START_DATE_TIME,
	COALESCE(ee.ENDDATETIME,current_timestamp) AS RAW_EVENT_END_DATE_TIME,
	COALESCE(ee.ENDDATETIME,current_timestamp) as EVENT_END_DATE_TIME,
	round(datediff(microseconds,greatest(ee.STARTDATETIME, SD.START_TIME), coalesce(EVENT_END_DATE_TIME, current_timestamp()))/1000000) DURATION_SECONDS
	FROM {{ref('stg_equipmentevents')}} ee
	     INNER JOIN {{ref('stg_commontransform_shiftdates')}} SD
	     	ON COALESCE(ee.ENDDATETIME, current_timestamp) BETWEEN SD.PROCESSING_START_TIME AND SD.PROCESSING_END_TIME 
