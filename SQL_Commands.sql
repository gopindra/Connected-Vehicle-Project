/* Add new column to store event location as geometry */
ALTER TABLE driver_data
ADD COLUMN loc_geom geometry(Point, 4326);

/* Fill the newly created column with geometries */
UPDATE driver_data 
SET loc_geom = ST_SetSRID(ST_MakePoint((data->'location'->>'longitude')::numeric, (data->'location'->>'latitude')::numeric), 4326); -- 4 min to run

/* Add new column for Block Group ID */
ALTER TABLE driver_data
ADD COLUMN bg_id VARCHAR;

/* Finding matching block groups for all event locations */
UPDATE driver_data
SET bg_id = affgeoid
FROM texas_bg_2018
WHERE ST_Within(loc_geom, geom); -- 13 min 47 secs

/* Add column for timestamp */
ALTER TABLE driver_data
ADD COLUMN capturedTimestamp timestamptz;

/* Add timestamp in human form */
UPDATE driver_data
SET capturedTimestamp = to_timestamp((data->>'capturedTimestamp')::numeric / 1000);

/* Rectifying Time Zone */
SET timezone = 'America/Chicago';

/* Add column for local timestamp */
ALTER TABLE driver_data
ADD COLUMN local_time timestamp;

/* Update column with local timestamp */
UPDATE driver_data
SET local_time = timezone('America/Chicago', capturedTimestamp);

/* Add column for day hour */
ALTER TABLE driver_data
ADD COLUMN local_day_hour numeric;

/* Update column with day hour */
UPDATE driver_data
SET local_day_hour = EXTRACT(second from local_time) / 3600 +
					 EXTRACT(minute from local_time) / 60 +
					 EXTRACT(hour from local_time);

/* Add column for time of day */
ALTER TABLE driver_data
ADD COLUMN tod varchar(10);

/* Update column with time of day */
UPDATE driver_data
SET tod = CASE 
			WHEN (local_day_hour >= 0.0 AND local_day_hour < 6.5) OR 
				(local_day_hour >= 19.0 AND local_day_hour < 24) THEN 'Night'
			WHEN local_day_hour < 9.0 THEN 'AM Peak'
			WHEN local_day_hour < 12.0 THEN 'Midday 1'
			WHEN local_day_hour < 15.0 THEN 'Midday 2'
			WHEN local_day_hour < 19.0 THEN 'PM Peak'
			ELSE NULL;

/* Add day of week column */
ALTER TABLE driver_data
ADD COLUMN day_of_week integer;

/* Update column with day of week */
UPDATE driver_data
SET day_of_week = EXTRACT(dow from local_time);

/* Adding columns from jasonb attributes */
ALTER TABLE driver_data
ADD COLUMN device_id varchar,
ADD COLUMN journey_id varchar,
ADD COLUMN journey_event varchar,
ADD COLUMN zip_code integer;

/* Updating the columns */
UPDATE driver_data
SET device_id = data ->> 'deviceId',
	journey_id = data ->> 'journeyId',
	journey_event = data -> 'event' -> 'eventMetadata' ->> 'journeyEventType',
	zip_code = (data -> 'location' ->> 'postalCode')::integer;

/* Create table with journey events */
CREATE TABLE journey_data AS
	SELECT DISTINCT loc_geom, bg_id, local_time, local_day_hour, tod, day_of_week, device_id, journey_id, journey_event, zip_code -- There are rows that are completely duplicated and rows where only the dataPointID is different
	FROM driver_data
	WHERE data -> 'event' ->> 'eventType' = 'JOURNEY' AND
		  ((local_time >= '2020-03-02'::timestamp AND local_time < '2020-03-07'::timestamp) OR
	       (local_time >= '2020-04-06'::timestamp AND local_time < '2020-04-11'::timestamp));

/* Create trip table - table with rows having both start and end of journey */
CREATE TABLE trip_data AS
	SELECT 
		s.loc_geom as start_loc_geom, e.loc_geom as end_loc_geom,
		s.bg_id as start_bg_id, e.bg_id as end_bg_id,
		s.local_time as start_time, e.local_time as end_time,
		e.local_time - s.local_time as travel_time,
		s.tod, s.day_of_week, s.device_id,
		s.journey_id,
		s.journey_event as start_journey_event, e.journey_event as end_journey_event
	FROM journey_data as s, journey_data as e
	WHERE 
		s.device_id = e.device_id AND
		s.journey_id = e.journey_id AND
		s.journey_event = 'START' AND
		s.local_time < e.local_time
	ORDER BY 
		s.device_id, s.local_time;

/* Checking if journey start and end are as you would expect */
SELECT start_journey_event, end_journey_event, count(*) AS freq
FROM trip_data
GROUP BY start_journey_event, end_journey_event;

-- START -> END 1294622
-- START -> START 11624

/* Removing START -> START cases */
DELETE FROM trip_data
WHERE end_journey_event = 'START';

ALTER TABLE trip_data
DROP COLUMN start_journey_event, 
DROP COLUMN end_journey_event;

/* Creating variables for determining base location of vehicles */
ALTER TABLE trip_data
ADD COLUMN next_start_loc_geom geometry(Point, 4326),
ADD COLUMN next_bg_id varchar,
ADD COLUMN next_start_time timestamp,
ADD COLUMN overnight_stay boolean;

WITH cte AS (
	SELECT LEAD(start_loc_geom, 1) OVER(PARTITION BY device_id ORDER BY start_time) AS next_start_loc_geom,
	       LEAD(start_bg_id, 1) OVER(PARTITION BY device_id ORDER BY start_time) AS next_bg_id,
	       LEAD(start_time, 1) OVER(PARTITION BY device_id ORDER BY start_time) AS next_start_time
    FROM trip_data
    )
UPDATE trip_data
SET next_start_loc_geom = cte.next_start_loc_geom,
	next_bg_id = cte.next_bg_id,
	next_start_time = cte.next_start_time
FROM cte;