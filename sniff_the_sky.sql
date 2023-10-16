-- Sniff the Sky

/* To run the queries, while in the `curious_data` directory, first setup the
* db with `make clean && make`. Then to start querying, open a connection in
* your terminal to the database with `psql enceladus`. Copy these queries and
* run them in the terminal to see the results.  */

-- make sure to build the materialized view after running make clean && make
drop view if exists enceladus_events;
create materialized view enceladus_events as
select
events.id,
events.title,
events.description,
events.time_stamp,
events.time_stamp::date as date,
event_types.description as event,
to_tsvector(
  concat(events.description, '', events.title)
) as search
from events
inner join event_types
on event_types.id = events.event_type_id
where target_id=28
order by time_stamp;

create index idx_event_search
on enceladus_events using GIN(search);

-- the year/day of year for flybys
select date_part('year', date),
to_char(time_stamp, 'DDD') from enceladus_events
where event like '%closest%';

/*
 date_part | to_char
-----------+---------
      2005 | 048
      2005 | 068
      2005 | 195
      2008 | 072
      2008 | 224
      …
(24 rows)
*/


/*
Getting an error when running `make clean && make`:

CREATE TABLE
psql:/Users/mmcclure/Code/curious_data/build.sql:107: ERROR:  missing data for column "lst_s"
CONTEXT:  COPY inms, line 3339530: "2008-283T18:49:32.257,67772257,"ENCELADUS",-1027467,56216,232441,-23,"csn",0,"199-1",1,"High-EV","Of..."

With Nick’s help, deteremined that the issue was that the last row didn’t have
data for the last 8columns.
*/

-- Verify al the data needed can be cast properly
-- Timestamp and altitude for the ENCELADUS target
-- QUESTION: what is the (10,3) of numeric?
/*
It is precision then scale!
[datatype-numeric](https://www.postgresql.org/docs/current/datatype-numeric.html)
NUMERIC(3, 1) will round values to 1 decimal place and can store values between
-99.9 and 99.9, inclusive.
*/

select
(sclk::timestamp) as time_stamp,
alt_t::numeric(10,3) as altitude from import.inms
where target='ENCELADUS'
and alt_t IS NOT NULL;
/*
Verify that the timestamp is cast to UTC since that is what it is set to
according to the manifest
*/

/*
       time_stamp        | altitude
-------------------------+-----------
 2005-02-17 03:19:45.065 |  4318.183
 2005-02-17 03:19:45.099 |  4317.968
 2005-02-17 03:19:45.133 |  4317.754
 2005-02-17 03:19:45.167 |  4317.539
…
*/

-- convert to materialized view for speed
drop materialized view if exists flyby_altitudes;
create materialized view flyby_altitudes as
select
(sclk::timestamp) as time_stamp,
alt_t::numeric(10,3) as altitude from import.inms
where target='ENCELADUS'
and alt_t IS NOT NULL;

-- Get the lowest altitudes for each flyby
-- First, the altitude for a single day
select min(altitude) from flyby_altitudes
where time_stamp::date = '2005-02-17';
/*
   min
----------
 1272.075
(1 row)

1242 kilometers is the lowest altitude
Next, to see the lowest point of every flyby with the timestamp, remove the
where clause, add an order by, and remember that when doing aggregates and you
want to see other values along with it, you have to use GROUP BY
*/
select time_stamp, min(altitude)
from flyby_altitudes
group by time_stamp
order by min(altitude);
/*
No errors, but this is slow. Can mitigate it by creating a flybys table

My data seems off (check if Nick gets the same)

       time_stamp        |    min
-------------------------+-----------
 2008-03-12 19:06:11.526 |    50.292
 2008-03-12 19:06:11.492 |    50.292
 2008-03-12 19:06:11.56  |    50.293
 2008-03-12 19:06:11.458 |    50.293
 2008-03-12 19:06:11.594 |    50.294
 2008-03-12 19:06:11.424 |    50.295

^ these are my first rows. The book indicates though that the first row should
be:

       time_stamp        |    min
-------------------------+-----------
 2008-10-10 02:06:39.741 |   28.576

Next step: determine which altitude is the nadir (lowest point of the flyby)
Run an aggregate query and group by smaller subgroups
*/

select date_part('year', time_stamp) as year,
min(altitude) as nadir from flyby_altitudes
group by date_part('year', time_stamp);
/*
 year |  nadir
------+---------
 2005 | 168.012
 2008 |  50.292
(2 rows)

Dee had 7 rows.
Next add months to the query for a finer interval.
*/
select
date_part('year', time_stamp) as year,
date_part('month', time_stamp) as month,
min(altitude) as nadir from flyby_altitudes
group by
date_part('year', time_stamp),
date_part('month', time_stamp);
/*
 year | month |   nadir
------+-------+-----------
 2008 |     8 |    53.353
 2005 |     3 |   500.370
 2008 |    10 | 17971.584
 2005 |     7 |   168.012
 2005 |     2 |  1272.075
 2008 |     3 |    50.292
(6 rows)

Dee had 19 rows. We know there were 23 flybys, so that is the number we are striving for.
Dee getting 19 was the result of lumping together lowest results that occurred
in the same month & year, which happened 4 times.
Try querying by day:
*/

select
time_stamp::date as date,
min(altitude) as nadir from flyby_altitudes
group by time_stamp::date
order by date;
/*
Typo in the book: it has a `;` after the group by time_stamp clause
    date    |   nadir
------------+-----------
 2005-02-17 |  1272.075
 2005-03-09 |   500.370
 2005-07-14 |   168.012
 2008-03-12 |    50.292
 2008-08-11 |    53.353
 2008-10-09 | 17971.584
(6 rows)

Dee got 25 rows and sees that there are 2 sets of consecutive dates which is
impossible since flybys must be separated by ~2 weeks so Cassini can slingshot
around Titan or Saturn. These flybys happen around midnight and are being
spread across two calendar days. Let’s try by week.
*/
select
date_part('year', time_stamp) as year,
date_part('week', time_stamp) as week,
min(altitude) as altitude from flyby_altitudes
group by
date_part('year', time_stamp),
date_part('week', time_stamp);
/*
 year | week | altitude
------+------+-----------
 2008 |   41 | 17971.584
 2005 |    7 |  1272.075
 2005 |   28 |   168.012
 2005 |   10 |   500.370
 2008 |   33 |    53.353
 2008 |   11 |    50.292
(6 rows)

Dee sees 23 rows! Each date corresponds with the published flyby dates.
I only had about 3 million rows imported, and Dee had 13 million. So I think my
CSV is just missing data.
Next: get the exact timestamp for each nadir
*/

-- Transforming Data with CTEs
/*
CTE: Common Table Expression
-> allow you to chain queries together, passing the result of one to the next, similar to piping on the command line. “Functional SQL”
Goal:
- get all the low altitude data, grouped by year & week (already done)
- get all timestamps associated with that year, week, and altitude
- pick one
*/

with lows_by_week as (
select date_part('year', time_stamp) as year,
date_part('week', time_stamp) as week,
min(altitude) as altitude from flyby_altitudes
group by date_part('year', time_stamp), date_part('week', time_stamp);
), nadirs as(
--?
)
select * from nadirs;

-- What do we put in the nadirs as block? Test a sample run:
select time_stamp as nadir, altitude from flyby_altitudes
where flyby_altitudes.altitude=50.292
and date_part('year', time_stamp)=2008
and date_part('week', time_stamp)=11;
/*
          nadir          | altitude
-------------------------+----------
 2008-03-12 19:06:11.492 |   50.292
 2008-03-12 19:06:11.526 |   50.292
(2 rows)

Dee used 28.567, 2008, 41. Got back 2 rows.
Since Cassini flies so fast, and the INMS snaps readings every 30ms or so, it’s possible to have some timestamps returned with the exact elevation being used as a filter. Take the minimum timestamp and subtract it from the maximum. This returns an interval that we can divide in half and add back to the minimum to calculate the midway point.
*/
select
min(time_stamp) + (max(time_stamp) - min(time_stamp))/2
as nadir,
altitude
from flyby_altitudes
where flyby_altitudes.altitude=50.292
and date_part('year', time_stamp)=2008
and date_part('week', time_stamp)=11
group by altitude;
/*
          nadir          | altitude
-------------------------+----------
 2008-03-12 19:06:11.509 |   50.292
(1 row)

Dee got 2008-10-10 02:06:39.724 28.576
Now we can plug this in to the above CTE
*/

with lows_by_week as (
select date_part('year', time_stamp) as year,
date_part('week', time_stamp) as week,
min(altitude) as altitude from flyby_altitudes
group by date_part('year', time_stamp), date_part('week', time_stamp)
), nadirs as (
select (
  min(time_stamp) + (max(time_stamp) - min(time_stamp))/2
) as nadir,
lows_by_week.altitude
from flyby_altitudes, lows_by_week
where flyby_altitudes.altitude = lows_by_week.altitude
and date_part('year', time_stamp) = lows_by_week.year
and date_part('week', time_stamp) = lows_by_week.week
group by lows_by_week.altitude
order by nadir
)
select nadir at time zone 'UTC', altitude from nadirs;
/*
          timezone           | altitude
-----------------------------+-----------
 2005-02-17 03:30:12.119+00  |  1272.075
 2005-03-09 09:08:03.4725+00 |   500.370
 2005-07-14 19:55:22.33+00   |   168.012
 2008-03-12 19:06:11.509+00  |    50.292
 2008-08-11 21:06:18.574+00  |    53.353
 2008-10-09 18:49:32.257+00  | 17971.584
(6 rows)

Dee got 23 rows and the first column was “nadir” but it is basically the timestamps. End of “Sniff the sky”!
*/
