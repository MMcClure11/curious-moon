-- make sure to setup the flybys table following the steps in "ring dust" chapter

-- alter the flybys table to make room for new stuff
alter table flybys
add speed_kms numeric(10,3),
add target_altitude numeric(10,3),
add transit_distance numeric(10,3);

-- calculate b
select id, altitude, (altitude + 252) as total_altitude --b
from flybys;

/*
 id | altitude  | total_altitude
----+-----------+----------------
  1 | 17971.584 |      18223.584
  2 |    53.353 |        305.353
  3 |   168.012 |        420.012
  4 |    50.292 |        302.292
  5 |   500.370 |        752.370
  6 |  1272.075 |       1524.075
*/

-- calculate b and calculate
-- Postgres has a sin function but it takes radians instead of degrees as an argument
-- For degrees we have to use `sind` function
select id, altitude,
  (altitude + 252) as total_altitude, -- b
  ((altitude + 252) / sind(73)) - 252 as target_altitude -- c
from flybys;
/*
 id | altitude  | total_altitude |  target_altitude
----+-----------+----------------+--------------------
  1 | 17971.584 |      18223.584 | 18804.251562451085
  2 |    53.353 |        305.353 |  67.30511491862012
  3 |   168.012 |        420.012 | 187.20308602568002
  4 |    50.292 |        302.292 |  64.10425245201293
  5 |   500.370 |        752.370 |  534.7471068282356
  6 |  1272.075 |       1524.075 | 1341.7126637681502
*/
-- updating flybys table with speed data
update flybys
set target_altitude=(
  (altitude + 252) / sind(73)
) - 252;

update flybys
set transit_distance=(
  (target_altitude + 252) * sind(17) * 2
);

/*
Query the flyby_altitudes view using the target_altitude to determine start and
end times for crossing the analysis window
Can't just query for the timestamps because the computed value doesn't match exactly what's in the view
*/
-- start time
update flybys set start_time=(
  -- typo in book, should be timstamp
  select min(timestamp)
  from flyby_altitudes
  where flybys.start_time::date
  = flyby_altitudes.timestamp::date
  and altitude < flybys.target_altitude + 0.75
  and altitude > flybys.target_altitude - 0.75
);
-- end time
update flybys set start_time=(
  select min(timestamp)
  from flyby_altitudes
  where flybys.start_time::date
  = flyby_altitudes.timestamp::date
  and altitude < flybys.target_altitude + 0.75
  and altitude > flybys.target_altitude - 0.75
);

select * from flyby_altitudes
where timestamp::date='2005-02-17'
and altitude between 1200 and 1500
order by timestamp;
/*
        timestamp        | year | week | altitude
-------------------------+------+------+----------
 2005-02-17 03:29:00.414 | 2005 |    7 | 1380.554
 2005-02-17 03:29:00.448 | 2005 |    7 | 1380.471
 2005-02-17 03:29:00.482 | 2005 |    7 | 1380.388
 2005-02-17 03:29:00.516 | 2005 |    7 | 1380.305
 2005-02-17 03:29:00.55  | 2005 |    7 | 1380.222
 2005-02-17 03:29:00.584 | 2005 |    7 | 1380.139
 2005-02-17 03:29:00.618 | 2005 |    7 | 1380.057
 2005-02-17 03:29:00.652 | 2005 |    7 | 1379.974
 2005-02-17 03:29:00.686 | 2005 |    7 | 1379.891
etc
Dee is missing readings between 1273 and 1375 km
Turns out we had incorrect assumptions and need a different data source
*/

drop table if exists flybys;
create table flybys(
  id int primary key,
  name text not null,
  date date not null,
  altitude numeric(7,1),
  speed numeric(7,1)
);

copy flybys from '/Users/mmcclure/Code/curious_data/data/jpl_flybys.csv' delimiter ',' header csv;

/*
Select * from flybys;
 id | name |    date    | altitude | speed
----+------+------------+----------+-------
  1 | E-0  | 2005-02-17 |          |
  2 | E-1  | 2005-03-09 |    504.0 |
  3 | E-2  | 2005-07-14 |    172.0 |   8.2
  4 | E-3  | 2008-03-12 |     52.0 |  14.4
  5 | E-4  | 2008-08-11 |     50.0 |  17.7
  6 | E-5  | 2008-10-09 |     25.0 |  17.7
  7 | E-6  | 2008-10-31 |    197.0 |  17.7
  8 | E-7  | 2009-11-02 |    103.0 |   7.7
  9 | E-8  | 2009-11-21 |   1606.0 |   7.7
 10 | E-9  | 2010-04-28 |    100.0 |   6.5
 11 | E-10 | 2010-05-18 |    438.0 |   6.5
 12 | E-11 | 2010-08-13 |   2502.0 |   6.8
 13 | E-12 | 2010-11-30 |     47.9 |   6.3
 14 | E-13 | 2010-12-21 |     47.8 |   6.2
 15 | E-14 | 2011-10-01 |     99.0 |   7.4
 16 | E-15 | 2011-10-19 |   1231.0 |   7.4
 17 | E-16 | 2011-11-06 |    496.0 |   7.4
 18 | E-17 | 2012-03-27 |     74.0 |   7.5
 19 | E-18 | 2012-04-14 |     74.0 |   7.5
 20 | E-19 | 2012-05-02 |     74.0 |   7.5
 21 | E-20 | 2015-10-14 |   1839.0 |   8.5
 22 | E-21 | 2015-10-28 |     49.0 |   8.5
 23 | E-22 | 2015-12-19 |   4999.0 |   9.5
(23 rows)
*/

/*
can create a temporary table which is less SQL to create a table then delete it
when finished
sclk column is the "Spacecraft clock"
*/
drop table if exists time_altitudes;
select
  (sclk::timestamp) as timestamp,
  alt_t::numeric(9,2) as altitude,
  date_part('year',(sclk::timestamp)) as year,
  date_part('week',(sclk::timestamp)) week
into time_altitudes
from import.inms
where target='ENCELADUS'
and alt_t IS NOT null;

select min(altitude) as nadir, year, week
from time_altitudes
group by year, week order by year, week;
/*
 nadir   | year | week
----------+------+------
  1272.08 | 2005 |    7
   500.37 | 2005 |   10
   168.01 | 2005 |   28
    50.29 | 2008 |   11
    53.35 | 2008 |   33
 17971.58 | 2008 |   41
(6 rows)
*/

-- wrap it in a CTE to create a flaybys table 2.0
with mins as (
  select min(altitude) as nadir, year, week
  from time_altitudes
  group by year, week
  order by year, week
), min_times as (
  select mins.*, min(timestamp) as low_time,
    min(timestamp) + interval '20 seconds' as window_end,
    min(timestamp) - interval '20 seconds' as window_start
  from mins
    inner join time_altitudes ta on mins.year = ta.year
    and mins.week = ta.week
    and mins.nadir = ta.altitude
  group by mins.week, mins.year, mins.nadir
), fixed_flybys as (
  select f.id, f.name, f.date, f.altitude, f.speed, mt.nadir, mt.year, mt.low_time, mt.window_start, mt.window_end
  from flybys f
  inner join min_times mt on
  date_part('year', f.date) = mt.year and
  date_part('week', f.date) = mt.week
)

-- To create the new flybys table
-- create the table from the CTE
select * into flybys_2
from fixed_flybys
order by date;

-- add a primary key
alter table flybys_2
add primary key (id);

-- drop the flybys table
drop table flybys cascade;
drop table time_altitudes;

-- rename flybys_2
alter table flybys_2
rename to flybys;

-- add a targeted field
alter table flybys
add targeted boolean not null default false;

-- set it
update flybys
set targeted = true
where id in (3,5,7,17,18,21);
