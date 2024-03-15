# <center> Homework: RisingWave Workshop </center>
---

## Environment

<details>
<summary>Setting up the environment</summary>

```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"

```
</details>

---
---

## Question 0

> What are the dropoff taxi zones at the latest dropoff times?
>
> For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).


#### Solution 0

<details>
<summary>Materialized View 0</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone,         latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```
</details>

---
---

## Question 1

> Create a materialized view to compute the average, min and max trip time between each taxi zone.
>
> From this MV, find the pair of taxi zones with the highest average trip time:


<details>
<summary>Materialized View 1</summary>

```sql
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
```
</details>

---

#### Solution 1
- [X] `Yorkville East -> Steinway`
- [ ] `Murray Hill -> Midwood`
- [ ] `East Flatbush/Farragut -> East Harlem North`
- [ ] `Midtown Center -> University Heights/Morris Heights`

---

> Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute, but the max trip time is 10 minutes and 20 minutes respectively.

<details>
<summary>Bonus</summary>

```sql
CREATE MATERIALIZED VIEW find_anomalies AS
    SELECT
        pickup_zone,
        dropoff_zone,
        round (average_trip_time, 1) AS average_trip_time,
        round (min_trip_time, 1) AS min_trip_time,
        round (max_trip_time, 1) AS max_trip_time,
        total_trips
    FROM ()
    ;
```
</details>

---
---

## Question 2

> Recreate the Materialized View(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

#### Solution 2
- [ ] 5
- [ ] 3
- [ ] 10
- [x] 1

---
---

## Question 3

> From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
>
> For example if the latest pickup time is `2020-01-01 17:00:00`, then the query should return the top 3 busiest zones from `2020-01-01 00:00:00` to `2020-01-01 17:00:00`.

<details>
<summary>Materialized View 3</summary>

```sql
CREATE MATERIALIZED VIEW busiest_zones AS
    SELECT
        taxi_zone.Zone AS pickup_zone,
        COUNT(*) AS total_pickups
    FROM trip_data
    JOIN taxi_zone
        ON trip_data.PULocationID = taxi_zone.location_id
    WHERE tpep_pickup_datetime > (SELECT MAX(tpep_pickup_datetime) - INTERVAL '17 hours' FROM trip_data)
    GROUP BY pickup_zone
    ORDER BY total_pickups DESC
    LIMIT 5;
```
</details>

---

#### Solution 3
- [ ] `Clinton East`, `Upper East Side North`, `Penn Station`
- [x] `LaGuardia Airport`, `Lincoln Square East`, `JFK Airport`
- [ ] `Midtown Center`, `Upper East Side South`, `Upper East Side North`
- [ ] `LaGuardia Airport`, `Midtown Center`, `Upper East Side North`

---
---