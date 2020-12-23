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
			ELSE NULL
		  END;

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
		s.journey_id
	FROM journey_data as s, journey_data as e
	WHERE 
		s.device_id = e.device_id AND
		s.journey_id = e.journey_id AND
		s.journey_event = 'START' AND
		e.journey_event = 'END' AND
		s.local_time < e.local_time
	ORDER BY 
		s.device_id, s.local_time;

/* Are there only two entries for every journey ID? */
SELECT 
	CASE 
	WHEN COUNT(DISTINCT journey_id) = COUNT(*) THEN 'journey IDs are DISTINCT'
	ELSE 'journey IDs are not DISTINCT'
	END
FROM trip_data;

/* Show journey IDs that occur multiple times */
SELECT *
FROM trip_data
WHERE journey_id IN (
	SELECT journey_id 
	FROM trip_data
	GROUP BY journey_id
	HAVING COUNT(*) > 1
)
ORDER BY journey_id, start_time;
-- 17875 rows selected
-- Many of these journeys occur over overlapping time periods.
-- No easy way to select a time period

/* Heuristic: Only select journeys with travel times closest to half an hour. */
/* There is probably a better way to do this */
CREATE TABLE trip_data2 AS
	WITH cte AS (
		SELECT journey_id, MIN(greatest(travel_time - '30 minutes'::interval, '30 minutes'::interval - travel_time)) AS opt_interval_diff
		FROM trip_data
		GROUP BY journey_id
	)
	SELECT DISTINCT ON (trip_data.journey_id) trip_data.*
	FROM trip_data
		INNER JOIN cte
		ON trip_data.journey_id = cte.journey_id AND
			greatest(trip_data.travel_time - '30 minutes'::interval, '30 minutes'::interval - trip_data.travel_time) = cte.opt_interval_diff
	ORDER BY trip_data.journey_id, trip_data.start_time;

DROP TABLE trip_data;
ALTER TABLE trip_data2 RENAME TO trip_data;

/* Now every row should have distinct journey IDs */
ALTER TABLE trip_data ADD PRIMARY KEY (journey_id);

/* Deleting duplicate trips with different journey_id */
WITH count_table AS (
	SELECT journey_id, ROW_NUMBER() OVER(PARTITION BY device_id, start_loc_geom, end_loc_geom, start_time, end_time ORDER BY journey_id) AS num
	FROM trip_data
)
DELETE FROM trip_data
WHERE journey_id IN (
		SELECT journey_id 
		FROM count_table
		WHERE num > 1
	);

/* Heuristic: Deleting rows with same start time but different ending times - keep rows with travel_time closest to 30 min */
WITH cte AS (
	SELECT *, ROW_NUMBER() OVER(
		PARTITION BY device_id, start_time
		ORDER BY GREATEST(travel_time - '30 minutes'::interval, '30 minutes'::interval - travel_time)) AS tt_rank
	    -- So rows with tt_rank = 1 will have lowest absolute difference between travel_time and 30 min
    FROM trip_data
)
DELETE FROM trip_data
WHERE journey_id IN (
	SELECT journey_id
	FROM cte
	WHERE tt_rank > 1
);

/* Creating variables for determining base location of vehicles */
ALTER TABLE trip_data
ADD COLUMN next_start_loc_geom geometry(Point, 4326),
ADD COLUMN next_bg_id varchar,
ADD COLUMN next_start_time timestamp;

/* Creating columns for stay_indicator (next start BG same as end BG), stay_duration */
ALTER TABLE trip_data
ADD COLUMN stay_indicator boolean,
ADD COLUMN stay_duration interval;

WITH cte AS (
	SELECT journey_id,
		   LEAD(start_loc_geom, 1) OVER(PARTITION BY device_id ORDER BY start_time) AS next_start_loc_geom,
	       LEAD(start_bg_id, 1) OVER(PARTITION BY device_id ORDER BY start_time) AS next_bg_id,
	       LEAD(start_time, 1) OVER(PARTITION BY device_id ORDER BY start_time) AS next_start_time
    FROM trip_data
)
UPDATE trip_data
SET next_start_loc_geom = cte.next_start_loc_geom,
	next_bg_id = cte.next_bg_id,
	next_start_time = cte.next_start_time
FROM cte
WHERE cte.journey_id = trip_data.journey_id;

UPDATE trip_data
SET stay_indicator = end_bg_id = next_bg_id;

UPDATE trip_data
SET stay_duration = next_start_time - end_time;

/* Check cases with negative stay duration */
SELECT * FROM trip_data
WHERE stay_duration < '0 second'::interval;
-- 223 cases

/* Debug: View negative stay duration cases
----------------
WITH cte AS (
	SELECT ROW_NUMBER() OVER(ORDER BY device_id, start_time) AS num, *
	FROM trip_data
),
problem_rows AS (
	SELECT num
	FROM cte
	WHERE stay_duration < '0 seconds'::interval
)
SELECT *
FROM cte 
WHERE num IN (SELECT problem_rows.num FROM problem_rows) OR
	  num+1 IN (SELECT problem_rows.num FROM problem_rows) OR
	  num+2 IN (SELECT problem_rows.num FROM problem_rows) OR
	  num-1 IN (SELECT problem_rows.num FROM problem_rows) OR
	  num-2 IN (SELECT problem_rows.num FROM problem_rows) 
ORDER BY device_id, start_time;
-----------------
There seems to be no easy logic to fix these cases.
I will leave them as such.
*/

/* The stay indicators where stay duration is greater than 15 days are invalid because it must have occurred in a different month */
UPDATE trip_data
SET stay_indicator = CASE WHEN stay_duration < '15 days'::interval THEN stay_indicator ELSE NULL END;

/* Debug: Count cases with different stay indicators 
SELECT stay_indicator, count(*)
FROM trip_data
GROUP BY stay_indicator;
*/

/* Setting stay_duration to NULL if stay_indicator is false or NULL or if stay_duration is negative */
UPDATE trip_data
SET stay_duration = CASE 
		WHEN stay_duration < '15 days'::interval AND stay_duration >= '0 seconds'::interval THEN 
			CASE 
				WHEN stay_indicator THEN stay_duration
				ELSE NULL -- stay_indicator is FALSE or NULL
			END
    	ELSE NULL 
	END;

/* Creating column for overnight_stay (stay period covers 3 AM in the morning) */
ALTER TABLE trip_data
ADD COLUMN overnight_stay boolean;

/* Function to compute seconds from midnight */
CREATE OR REPLACE FUNCTION seconds_from_midnight(t timestamp)
 RETURNS int AS
$$
BEGIN
	RETURN (extract(second from t) +
			extract(minute from t) * 60 +
		 	extract(hour from t) * 60 * 60)::int;
END;
$$
LANGUAGE plpgsql;

/*Function to find next 3:00 AM timestamp that occurs at or after the input time stamp */
CREATE OR REPLACE FUNCTION get_day_end(t timestamp)
	RETURNS timestamp AS
$$
DECLARE sec_from_mn int; day_end_time timestamp;
BEGIN
	SELECT seconds_from_midnight(t) INTO sec_from_mn;
	SELECT
		CASE
		WHEN sec_from_mn <= 3*60*60
			THEN DATE(t) + '3:00'::interval
		WHEN sec_from_mn > 3*60*60
			THEN DATE(t) + '1 day 3:00'::interval
		ELSE NULL
		END
	INTO day_end_time;
	RETURN day_end_time;
END;
$$ LANGUAGE plpgsql;

UPDATE trip_data
SET overnight_stay = CASE WHEN stay_indicator AND stay_duration IS NOT NULL
						  THEN next_start_time > get_day_end(end_time)
				     ELSE NULL END;

/* Generate a table with the vehicles in the database */
/* Table has columns showing which block group the vehicle has spent maximum time at overall and on time periods containing 3 AM */
CREATE TABLE vehicle_data AS
	WITH tot_stay_cte AS (
		SELECT 
			device_id, 
			next_bg_id, SUM(stay_duration) AS stay_duration, 
			ROW_NUMBER() OVER(PARTITION BY device_id ORDER BY SUM(stay_duration) DESC) AS duration_rank
		FROM trip_data
		WHERE stay_duration IS NOT NULL
		GROUP BY device_id, next_bg_id
	),
	night_stay_cte AS (
		SELECT 
			device_id, 
			next_bg_id, SUM(stay_duration) AS stay_duration, 
			ROW_NUMBER() OVER(PARTITION BY device_id ORDER BY SUM(stay_duration) DESC) AS duration_rank
		FROM trip_data
		WHERE stay_duration IS NOT NULL AND overnight_stay
		GROUP BY device_id, next_bg_id
	)
	SELECT 
		t.device_id,
		t.stay_duration AS tot_stay_duration,
		t.next_bg_id AS tot_stay_bg_id,
		n.stay_duration AS night_stay_duration,
		n.next_bg_id AS night_stay_bg_id
	FROM 
		tot_stay_cte AS t, 
		night_stay_cte AS n
	WHERE 
		t.device_id = n.device_id AND
		t.duration_rank = 1 AND
		n.duration_rank = 1;