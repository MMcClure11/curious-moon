-- RING DUST


/* To run the queries, while in the `curious_data` directory, first setup the
* db with `make clean && make`. Then to start querying, open a connection in
* your terminal to the database with `psql enceladus`. Copy these queries and
* run them in the terminal to see the results.  */

-- BETTER CTE
/*
Speed calculation is next.
Will need new data from the CDA data.
CDA = Cosmic Dust Analyzer
Create a flybys table with the timestamp and altitude information, include
speed, start_time, end_time. Use a Makefile when calculating Cassini’s speed.
*/

-- refactor the materialized view and rebuild it with month & year information
drop materialized view if exists flyby_altitudes;
create materialized view flyby_altitudes as
select
  (sclk::timestamp) as timestamp,
  date_part('year', (sclk::timestamp)) as year,
  date_part('week', (sclk::timestamp)) as week,
  alt_t::numeric(10,3) as altitude
from import.inms
where target='ENCELADUS'
and alt_t IS NOT NULL;

-- simplify the CTE
-- Book typo, there should not be an underscore for the timestamp variables
with lows_by_week as (
  select year, week,
  min(altitude) as altitude
    from flyby_altitudes
    group by year, week
), nadirs as (
  select (
    min(timestamp) + (max(timestamp) - min(timestamp))/2) as nadir,
    lows_by_week.altitude
    from flyby_altitudes, lows_by_week
  where flyby_altitudes.altitude = lows_by_week.altitude
  and flyby_altitudes.year = lows_by_week.year
  and flyby_altitudes.week = lows_by_week.week
  group by lows_by_week.altitude
  order by nadir
)
  select nadir, altitude from nadirs;

/*
          nadir           | altitude
--------------------------+-----------
 2005-02-17 03:30:12.119  |  1272.075
 2005-03-09 09:08:03.4725 |   500.370
 2005-07-14 19:55:22.33   |   168.012
 2008-03-12 19:06:11.509  |    50.292
 2008-08-11 21:06:18.574  |    53.353
 2008-10-09 18:49:32.257  | 17971.584
(6 rows)
*/

-- The above is better, but can be improved. Calculations in SQL are a code smell. They can almost always be simplified into a fuction.
-- The body of a function is delimited by $$, what is returned from the query gets popped into the output variable
drop function if exists low_time(numeric, double_precision);
create function low_time(
  alt numeric,
  yr double precision,
  wk double precision,
  out timestamp without time zone
)
as $$
  select min(timestamp) + ((max(timestamp) - min(timestamp))/2) as nadir
  from flyby_altitudes
  where flyby_altitudes.altitude = alt
  and flyby_altitudes.year = yr
  and flyby_altitudes.week = wk
$$ language sql;

-- Update the CTE to use the above function
with lows_by_week as (
  select year, week, min(altitude) as altitude
  from flyby_altitudes
  group by year, week
), nadirs as (
  select low_time(altitude, year, week) as timestamp,
  altitude from lows_by_week
)
  select * from nadirs;

/*
        timestamp         | altitude
--------------------------+-----------
 2008-10-09 18:49:32.257  | 17971.584
 2008-08-11 21:06:18.574  |    53.353
 2005-07-14 19:55:22.33   |   168.012
 2008-03-12 19:06:11.509  |    50.292
 2005-03-09 09:08:03.4725 |   500.370
 2005-02-17 03:30:12.119  |  1272.075
(6 rows)
*/

-- FLYBYS TABLE
-- From this result we can now make a flybys table
-- convenience for redoing
drop table if exists flybys;

-- redone CTE
with lows_by_week as (
  select year, week, min(altitude) as altitude
  from flyby_altitudes
  group by year, week
), nadirs as (
  select low_time(altitude, year, week) as timestamp,
  altitude from lows_by_week
)
-- exec the CTE, pushing results into flybys
select nadirs.*,
  -- set initial values to NULL
  null::varchar as name,
  null::timestamp as start_time,
  null::timestamp as end_time
-- push into a new table
into flybys from nadirs;
-- ^ book says timestampz, but that produced an error
-- ERROR:  type "timestampz" does not exist
-- LINE 13:   null::timestampz as start_time,

-- add a primary key
alter table flybys
add column id serial primary key;

-- using the key, create the name using the new id
-- || concatenates strings and coerces to string
update flybys set name='E-' || id-1;

-- IMPORT CDA
/*
see updates to Makefile, import.sql, build.sql
Dee did not add it to her Makefile, but since I already have the pattern
established, I went ahead and continued with that.
Ah shit. But the cda imports table is empty :(
psql enceladus < cda_import.sql
So to get it to work, I instead made a `cda_import.sql` file which makes the
table and schema. Running the select below is now returning the expected
results!
*/

-- EXAMINING CDA DATA
select * from cda.impacts where x_velocity <> -99.99 limit 5;
/*
   id   |       time_stamp       | impact_date | counter | sun_distance_au | saturn_distance_rads | x_velocity | y_velocity | z_velocity | particle_charge | particle_mass
--------+------------------------+-------------+---------+-----------------+----------------------+------------+------------+------------+-----------------+---------------
 398120 | 2005-01-01 00:02:42-03 | 2005-01-01  |      37 |          9.0501 |                59.63 |      -8.38 |      -4.45 |      -1.08 |             0.0 |           0.0
 398121 | 2005-01-01 00:04:55-03 | 2005-01-01  |      19 |          9.0501 |                59.63 |      -8.38 |      -4.45 |      -1.08 |             0.0 |           0.0
 398122 | 2005-01-01 00:07:36-03 | 2005-01-01  |      37 |          9.0501 |                59.63 |      -8.38 |      -4.45 |      -1.08 |             0.0 |           0.0
 398123 | 2005-01-01 00:09:24-03 | 2005-01-01  |      37 |          9.0501 |                59.63 |      -8.38 |      -4.45 |      -1.08 |             0.0 |           0.0
 398124 | 2005-01-01 00:13:05-03 | 2005-01-01  |      37 |          9.0501 |                59.63 |      -8.38 |      -4.45 |      -1.08 |             0.0 |           0.0
(5 rows)
*/

-- CALCULATING CASSINI’S SPEED
select time_stamp,
  x_velocity,
  y_velocity,
  z_velocity,
  sqrt(
    (x_velocity * x_velocity) +
    (y_velocity * y_velocity) +
    (z_velocity * z_velocity)
  )::numeric(10,2) as v_kms from cda.impacts
where x_velocity <>  -99.99;
/*
       time_stamp       | x_velocity | y_velocity | z_velocity | v_kms
------------------------+------------+------------+------------+-------
 2005-01-01 00:02:42-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:04:55-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:07:36-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:09:24-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:13:05-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:20:26-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:20:27-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:24:20-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:26:10-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:31:39-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
 2005-01-01 00:32:06-03 |      -8.38 |      -4.45 |      -1.08 |  9.55
Lots of rows!
The speed calculated is relative to the sun.
*/

-- PLAYING WITH SPEED DATA
-- pythag function
drop function if exists pythag(numeric, numeric, numeric);
create function pythag(
  x numeric,
  y numeric,
  z numeric, out numeric)
as $$
  select sqrt((x * x) + (y * y) + (z * z))::numeric(10,2);
$$ language sql;

-- using pythag function
select impact_date, pythag(x_velocity, y_velocity, z_velocity) as v_kms
from cda.impacts where x_velocity <> -99.99;
/*
 impact_date | v_kms
-------------+-------
 2005-01-01  |  9.55
 2005-01-01  |  9.55
 2005-01-01  |  9.55
 2005-01-01  |  9.55
 2005-01-01  |  9.55
 2005-01-01  |  9.55
 2005-01-01  |  9.55
 2005-01-01  |  9.55
MANY ROWS
Calculates the true speed for Cassini relative to the sun, displays it as km
per second along with a timestamp
*/

-- Want to also see miles per hour
with kms as (
  select impact_date as the_date,
  date_part('month', time_stamp) as month,
  date_part('year', time_stamp) as year,
  pythag(x_velocity, y_velocity, z_velocity) as v_kms
  from cda.impacts
  where x_velocity <> -99.99
), speeds as (
  select kms.*,
  (v_kms * 60 * 60)::integer as kmh,
  (v_kms * 60 * 60 * .621)::integer as mph
  from kms
)
  select * from speeds;
/*
  the_date  | month | year | v_kms |  kmh  |  mph
------------+-------+------+-------+-------+-------
 2005-01-01 |     1 | 2005 |  9.55 | 34380 | 21350
 2005-01-01 |     1 | 2005 |  9.55 | 34380 | 21350
 2005-01-01 |     1 | 2005 |  9.55 | 34380 | 21350
 2005-01-01 |     1 | 2005 |  9.55 | 34380 | 21350
 2005-01-01 |     1 | 2005 |  9.55 | 34380 | 21350
 2005-01-01 |     1 | 2005 |  9.55 | 34380 | 21350
 2005-01-01 |     1 | 2005 |  9.55 | 34380 | 21350
 2005-01-01 |     1 | 2005 |  9.55 | 34380 | 21350
 2005-01-01 |     1 | 2005 |  9.55 | 34380 | 21350
Many more rows!
*/

-- WINDOW FUNCTIONS
/*
A window function performs a calculation across a set of
table rows that are somehow related to the current row.
This is comparable to the type of calculation that can be
done with an aggregate function. However, window functions
do not cause rows to become grouped into a single output
row like non-window aggregate calls would.

Window functions are basically GROUP BY queries without the group by.
*/

-- simple window function that gets a count of mission plan events for each target
select targets.description,
count(1) over (
  partition by targets.description
  )
  from events
inner join targets on targets.id = target_id;
/*
       description       | count
-------------------------+-------
 Aegaeon                 |     4
 Aegaeon                 |     4
 Aegaeon                 |     4
 Aegaeon                 |     4
 Anthe                   |     1
 Atlas                   |     9
 Atlas                   |     9
 Atlas                   |     9

Repeated rows because of the lack of a GROUP BY
*/

-- To get a count of all the events, leave out the partition clause
select targets.description,
count(1) over ()
  from events
inner join targets on targets.id = target_id;
/*
       description       | count
-------------------------+-------
 Phoebe                  | 61873
 Dione                   | 61873
 Rhea                    | 61873
 Rhea                    | 61873
 Rhea                    | 61873
 Enceladus               | 61873
 Enceladus               | 61873
*/

-- percentile of target distribution
select targets.description as target,
  100.0 *
  (
  (count(1) over (
    partition by targets.description
  ))::numeric /
  (
    count(1) over ()
  )::numeric ) as percent_of_mission
  from events
inner join targets on targets.id = target_id;
/*
         target          |     percent_of_mission
-------------------------+-----------------------------
 Aegaeon                 | 0.0064648554296704539945000
 Aegaeon                 | 0.0064648554296704539945000
 Aegaeon                 | 0.0064648554296704539945000
 Aegaeon                 | 0.0064648554296704539945000
 Anthe                   | 0.0016162138574176134986000
 Atlas                   |     0.014545924716758521000
 Atlas                   |     0.014545924716758521000
 Atlas                   |     0.014545924716758521000
 Atlas                   |     0.014545924716758521000
 Atlas                   |     0.014545924716758521000
 Atlas                   |     0.014545924716758521000
 Atlas                   |     0.014545924716758521000
 Atlas                   |     0.014545924716758521000
 Atlas                   |     0.014545924716758521000
 Calypso                 |     0.019394566289011362000

Can use select distinct to return non-repeated results
*/

-- isolating with distinct
select distinct(targets.description) as target,
  100.0 *
  (
    (count(1) over ( partition by targets.description))::numeric /
    ( count(1) over ())::numeric
  ) as percent_of_mission
  from events
inner join targets on targets.id = target_id
order by percent_of_mission desc;

/*
         target          |     percent_of_mission
-------------------------+-----------------------------
 Saturn                  |    27.407754594087889710000
 Titan                   |    15.358880287039581077000
 Other                   |     9.634250804066394065000
 rings(general)          |     9.386970083881499200000
 InstrumentCalibration   |     7.352156837392723805000
 SolarWind               |     6.531120197824576148000
 DustRAM direction       |     4.328220710164368949000
 co-rotation             |     4.326604496306951336000
 Enceladus               |     2.627963732161039549000
*/

-- looking at teams
select distinct(teams.description) as team,
  100.0 *
  (
    (count(1) over ( partition by teams.description))::numeric /
    ( count(1) over ())::numeric
  ) as percent_of_mission
  from events
inner join teams on teams.id = team_id
order by percent_of_mission desc;
/*
 team  |    percent_of_mission
-------+--------------------------
 CIRS  | 19.344463659431415965000
 UVIS  | 14.882097199101385095000
 ISS   | 14.528146364326927739000
 VIMS  | 10.304979554894703667000
 INMS  |  7.672167181161411278000
 CDA   |  7.216394873369644271000
 RPWS  |  6.789714415011394308000
 CAPS  |  5.432094774780598969000
 MAG   |  5.049052090572624570000
 MIMI  |  4.512469089909976888000
 RSS   |  2.044510529633281076000
 RADAR |  1.323679149225025455000
 MP    |  0.885685193864852197000
 PROBE |  0.014545924716758521000
(14 rows)

Can do the same thing with our CTE, instead of count, get the min and max for a
given month.
*/

-- min/max speeds
with kms as (
  select impact_date as the_date,
  date_part('month', time_stamp) as month,
  date_part('year', time_stamp) as year,
  pythag(x_velocity, y_velocity, z_velocity) as v_kms
  from cda.impacts
  where x_velocity <> -99.99
), speeds as (
  select kms.*,
  (v_kms * 60 * 60)::integer as kmh,
  (v_kms * 60 * 60 * .621)::integer as mph
  from kms
), rollup as (
  -- The line below has a syntax error that I can’t figure out. :/
select year, month, max(mph) over (partition by month), min(mph) over (partition by month) from speeds
)

select * from rollup;

/*
Returns the hight and low speeds for each month of the Enceladus flybys.
*/
