-- Set the timezone to Mountain Standard Time (MST)
SET TIME ZONE 'MST';

/**
 Materialized View 0: Latest 1 minute trip data
**/

DROP MATERIALIZED VIEW IF EXISTS latest_1min_trip_data;

CREATE MATERIALIZED VIEW latest_1min_trip_data AS
    SELECT taxi_zone.Zone as pickup_zone,
           taxi_zone_1.Zone as dropoff_zone,
           tpep_pickup_datetime,
           tpep_dropoff_datetime
    FROM trip_data
    JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
    JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id
    WHERE tpep_dropoff_datetime > now() - interval '1 minute';

/**
 Materialized View 1: Total Airport Pickups
**/

DROP MATERIALIZED VIEW IF EXISTS total_airport_pickups;

CREATE MATERIALIZED VIEW total_airport_pickups AS
    SELECT
        taxi_zone.Zone as pickup_zone,
        count(*) as count
    FROM
        trip_data
            JOIN taxi_zone
                ON trip_data.PULocationID = taxi_zone.location_id
    WHERE taxi_zone.Zone LIKE '%Airport'
    GROUP BY taxi_zone.Zone;

/**
 Materialized View 2: Airport pickups from JFK Airport, 1 hour before the latest pickup
**/

DROP MATERIALIZED VIEW IF EXISTS airport_pu;

CREATE MATERIALIZED VIEW airport_pu as
    SELECT
        tpep_pickup_datetime,
        pulocationid
    FROM
        trip_data
            JOIN taxi_zone
                ON trip_data.PULocationID = taxi_zone.location_id
    WHERE
            taxi_zone.Borough = 'Queens'
    AND taxi_zone.Zone = 'JFK Airport';


DROP MATERIALIZED VIEW IF EXISTS latest_jfk_pickup;

CREATE MATERIALIZED VIEW latest_jfk_pickup AS
    SELECT
        max(tpep_pickup_datetime) AS latest_pickup_time
    FROM
        trip_data
            JOIN taxi_zone
                ON trip_data.PULocationID = taxi_zone.location_id
    WHERE
        taxi_zone.Borough = 'Queens'
      AND taxi_zone.Zone = 'JFK Airport';


DROP MATERIALIZED VIEW IF EXISTS jfk_pickups_1hr_before;

CREATE MATERIALIZED VIEW jfk_pickups_1hr_before AS
    SELECT
        count(*) AS count
    FROM
        airport_pu
            JOIN latest_jfk_pickup
                ON airport_pu.tpep_pickup_datetime > latest_jfk_pickup.latest_pickup_time - interval '1 hour'
            JOIN taxi_zone
                ON airport_pu.PULocationID = taxi_zone.location_id
    WHERE
        taxi_zone.Borough = 'Queens'
      AND taxi_zone.Zone = 'JFK Airport';

/**
 Materialized View 3: Top 10 busiest zones in the last 1 minute
**/

DROP MATERIALIZED VIEW IF EXISTS busiest_zones_1_min;

CREATE MATERIALIZED VIEW busiest_zones_1_min AS SELECT
    taxi_zone.Zone AS dropoff_zone,
    count(*) AS last_1_min_dropoff_count
FROM
    trip_data
        JOIN taxi_zone
            ON trip_data.DOLocationID = taxi_zone.location_id
WHERE
    trip_data.tpep_dropoff_datetime > (NOW() - INTERVAL '1' MINUTE)
GROUP BY
    taxi_zone.Zone
ORDER BY last_1_min_dropoff_count DESC
    LIMIT 10;
  
/**
 Materialized View 4: Longest trips
**/

DROP MATERIALIZED VIEW IF EXISTS longest_trip_1_min;

CREATE MATERIALIZED VIEW longest_trip_1_min AS
    SELECT
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        taxi_zone_pu.Zone as pickup_zone,
        taxi_zone_do.Zone as dropoff_zone,
        trip_distance
    FROM
        trip_data
    JOIN taxi_zone as taxi_zone_pu
        ON trip_data.PULocationID = taxi_zone_pu.location_id
    JOIN taxi_zone as taxi_zone_do
        ON trip_data.DOLocationID = taxi_zone_do.location_id
    WHERE
        trip_data.tpep_pickup_datetime > (NOW() - INTERVAL '5' MINUTE)
    ORDER BY
        trip_distance DESC
    LIMIT 10;

/**
 Materialized View 5: Average Fare Amount vs Number of rides
**/

DROP MATERIALIZED VIEW IF EXISTS avg_fare_amt;

CREATE MATERIALIZED VIEW avg_fare_amt AS
    SELECT
        avg(fare_amount) AS avg_fare_amount_per_min,
        count(*) AS num_rides_per_min,
        window_start,
        window_end
    FROM
        TUMBLE(trip_data, tpep_pickup_datetime, INTERVAL '1' MINUTE)
    GROUP BY
        window_start, window_end
    ORDER BY
        num_rides_per_min ASC;

/**
 Homework Materialized Views
**/

DROP MATERIALIZED VIEW IF EXISTS trip_minutes;

CREATE MATERIALIZED VIEW trip_minutes AS
    SELECT
        pickup_zone,
        dropoff_zone,
        round (average_trip_time, 1) AS average_trip_time,
        round (min_trip_time, 1) AS min_trip_time,
        round (max_trip_time, 1) AS max_trip_time,
        total_trips
    FROM (
        SELECT 
            pu_zone.Zone AS pickup_zone,
            do_zone.Zone AS dropoff_zone,
            AVG(
                EXTRACT(HOUR FROM (tpep_dropoff_datetime - tpep_pickup_datetime))*60*60
              + EXTRACT(MINUTE FROM (tpep_dropoff_datetime - tpep_pickup_datetime))
              + EXTRACT(SECOND FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60
            ) AS average_trip_time,
            MIN(
                EXTRACT(HOUR FROM (tpep_dropoff_datetime - tpep_pickup_datetime))*60*60
              + EXTRACT(MINUTE FROM (tpep_dropoff_datetime - tpep_pickup_datetime))
              + EXTRACT(SECOND FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60
            ) AS min_trip_time,
            MAX(
                EXTRACT(HOUR FROM (tpep_dropoff_datetime - tpep_pickup_datetime))*60*60
              + EXTRACT(MINUTE FROM (tpep_dropoff_datetime - tpep_pickup_datetime))
              + EXTRACT(SECOND FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60
            ) AS max_trip_time,
            COUNT(*) AS total_trips
        FROM trip_data
        JOIN taxi_zone as pu_zone
            ON trip_data.PULocationID = pu_zone.location_id
        JOIN taxi_zone as do_zone
            ON trip_data.DOLocationID = do_zone.location_id
        GROUP BY pickup_zone, dropoff_zone
    )
    ORDER BY average_trip_time DESC;