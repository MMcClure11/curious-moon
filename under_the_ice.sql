-- importing chemical data
create table chem_data(
 name text,
  formula varchar(10),
  molecular_weight integer,
  peak integer,
  sensitivity numeric
);

copy chem_data from '/Users/mmcclure/Code/curious_data/data/INMS/chem_data.csv' delimiter ',' header csv;

select * from chem_data;
-- woo! this worked! I'm getting the hang of this csv data importing. :)

-- load up new schema for instrument data
drop schema if exists inms cascade;
create schema inms;

-- move chem data in there
alter table chem_data set schema inms;

-- create the inms.readings table
select
  sclk::timestamp as timestamp, source::text, mass_table,
  alt_t::numeric(9,2) as altitude,
  mass_per_charge::numeric(6,3),
  p_energy::numeric(7,3),
  pythag(
  sc_vel_t_scx::numeric,
  sc_vel_t_scy::numeric,
  sc_vel_t_scz::numeric
  ) as relative_speed,
  c1counts::integer as high_counts,
  c2counts::integer as low_counts
into inms.readings
from import.inms
order by timestamp;
/*
tried a few different things, switching numerics to text but that meant the
pythag calculation wouldn't work, but I keep getting errors about certain
fields not being numeric
ERROR:  invalid input syntax for type numeric: "km/s"
ERROR:  invalid input syntax for type numeric: "Da/z"

Might need Nick's help, think I might be stuck at this point :( page 346 on mac book
Loading the INMS Readings Chapter
*/

alter table inms.readings
add id serial primary key;

-- Want to be able to get at this data easily by relating this table to flybys
-- add an FK to the flybys
alter table inms.readings
add flyby_id int references flybys(id);

-- this seems like an expensive operation,, maybe we can find out how ...
update inms.readings
set flyb_id=flybys.id
from flybys
where flybys.date = inms.readings.timestamp::date;

/*
You can add the keyword explain in front of a query and Postgres will do its
best to tell you what's going on
There are two options for updating the 13 million rows in inms.Readings
1. use a join
2. nested select statment
Let's compare 2 different ways of getting at the above
*/
explain update inms.readings
set flyb_id=flybys.id
from flybys
where flybys.date = inms.readings.timestamp::date;
-- cost is ~ 1.52 million

explain update inms.readings
set flyby_id = (
  select id from flybys
  where date = inms.readings.timestamp::date
  limit 1
);
/*
cost is ~ 74 million
can run with explain analyze which executes the query and reports what happens
there is a third option! Use a junction table
junction tables are a special kind of table that exists solely to support a
relationship between two unrelated tables which in our case are the
inms.readings and public.flybys table
*/

-- a junction table
drop table if exists flyby_readings;
create table flyby_readings(
  reading_id int not null unique references inms.readings(id),
  flyby_id int not null references flybys(id),
  primary key(reading_id, flyby_id)
);

-- fill the junction table
insert into flyby_readings(flyby_id, reading_id)
select
    flybys.id,
inms.readings.id
    from flybys
  inner join inms.readings
on date_part('year', timestamp) = flybys.year
  and date_part('week', timestamp) = flybys.week;

/*
wait, since the thing we are most interested in is the date, we can let
Postgres help us by just creating an index on the timestamp field instead of
using the join tables
We would want to do this to conserve disk space
"If propelling 'good' design makes you write needless code, take up unnecessary space, or make the system needlessly work harder, something's wrong."

TIMESTAMPS and INDEXES
Three timestamps to consider:
1. inms.readings.timestamp
2. flybys.window_start
3. flybys.window_end

Different approaches to structuring this data include:

A BTREE index
btree = balanced tree, a binary tree that has an equal distribution of child nodes on both sides of the primary node
You can add an index on the column that you are querying against to help Postgres find it
*/

create index idx_stamps on inms.readings(timestamp);
-- let's improve this
-- make Postgres do less work by restricting the index to only the values we'll be searching over, ie teh values with an altitude readings
create index idx_stamps on inms.readings(timestamp) where altitude is not null;
-- this will take a long time to go through 13 million rows, in production this would be bad as it would lock the entire table
-- use concurrently to avoid this
create index concurrently idx_stamps on inms.readings(timestamp) where altitude as not null;
-- now running a simple query with this index is fast
select * from inms.readings
where timestamp > '2015-12-19 17:48:55.275'
and timestamp < '2015-12-19 17:49:35:275'
and altitude is not null;
-- returns in 10ms!
-- explain shows how the index is being used instead of a sequential scan
-- now we can avoid hardcoding dates and instead join the flybys table using window_start and window_end
select
name,
  mass_per_charge,
  timestamp,
  inms.readings.altitude
  from inms.readings
inner join flybys
on timestamp >= window_start
and timestamp <= window_end
where flybys.id = 4;

-- name | mass_per_charge | time_stamp | altitude
--------------------------------------------------
-- E-3 | 25.000 | 2008-03-12 19:05:51.485 | 169.81
-- E-3 | 26.000 | 2008-03-12 19:05:51.521 | 169.46
-- E-3 | 27.000 | 2008-03-12 19:05:51.555 | 169.12

-- Include the name to make sure it aligns with dates and altitudes
-- add a countdown for altitude - nadir
select
name,
  mass_per_charge,
  timestamp,
  inms.readings.altitude,
  inms.readings.altitude - nadir as distance
  from inms.readings
inner join flybys
on timestamp >= window_start
and timestamp <= window_end
where flybys.id = 4;

-- makes the results more readable and provides perspective on how fast Cassini was going

-- name | mass_per_charge | time_stamp | distance
--------------------------------------------------
-- E-3 | 25.000 | 2008-03-12 19:05:51.485 | 119.52
-- E-3 | 26.000 | 2008-03-12 19:05:51.521 | 119.17
-- E-3 | 27.000 | 2008-03-12 19:05:51.555 | 118.83

-- Advanced: Using Time Ranges
-- Postgres supports datatypes dedicated to ranges, numeric ranges and date/time ranges
-- define a range with a simple constructor
-- instead of `window_start` and `window_end` can just use a column `analysis_window`
alter table flybys
add analysis_window tsrange;
-- to create a range invoke `tsrange` type
update flybys
set analysis_window=tsrange(window_start, window_end, '[]');
-- analysis_window
-------------------------
-- ["2005-02-17 03:29:52.119", "2005-02-17 03:30:32:119"]
-- ["2005-03-09 03:29:52.119", "2005-03-09 03:30:32:119"]
-- ["2005-03-12 03:29:52.119", "2005-03-12 03:30:32:119"]
-- [] option tells Postgres that the range is inclusive
-- ie the upper and lower bounds should be included
-- () option makes the range exclusive meaning all dates between the upper and lower bounds are consired the value
-- you can combine the options [) so this range is inclusive on the lower bound and exclusive on the upper
-- querying ranges has special operators you can use
-- check if a date is contained within a range
select name from flybys
where analysis_window @> '2005-02-17 03:30:12.119'::timestamp;
-- makes querying easier regarding the syntax
select
name,
  mass_per_charge,
  timestamp,
  inms.readings.altitude,
  inms.readings.altitude - nadir as distance
  from inms.readings
inner join flybys
on analysis_window @> inms.readings.timestamp
where flybys.id = 4;
-- this works but is really slow because it does 2 sequential scans
-- the @> operator can’t be used with a BTREE index
-- Queries such as overlap, exclusion, containment, existence, etc can be used with an index fo a tsrange
-- range types: https://wiki.postgresql.org/images/7/73/Range-types-pgopen-2012.pdf
-- Dee however is not querying a range directly

-- Comparing the Speeds
inner join inms.readings on
  time_stamp >= flybys.window_start and
  time_stamp <= flybys.window_end
group by speed;
-- id | name | speed | avg
-- 1 | E-0 |  | 6.6
-- 2 | E-1 |  | 6.6
-- 3 | E-2 | 8.2 | 8.2
-- 4 | E-3 | 14.4 | 14.4

-- Victory!  The speeds and average match an external source.

-- Putting it together: A Chemical Query
-- the chem_data table has a `peak` column which is AMU/Z (Daltons)
-- the INMS has the exact same unit type ith the mass_per_charge column
-- relate the two to find out which molecules were detected when
-- first look at chem results
select
  flybys.name,
  time_stamp,
  inms.readings.altitude,
  inms.chem_data.name as chem
from inms.readings
inner join flybys on
  time_stamp >= flybys.window_start
  and time_stamp <= flybys.window_end
inner join inms.chem_data on
  peak = mass_per_charge
where flybys.id = 4;

-- name | time_stamp | altitude | chem
-- E-3 | 2008-03-12 19:05:51.521 | 169.46 | Acetylene
-- E-3 | 2008-03-12 19:05:51.521 | 169.46 | Acetylene
-- E-3 | 2008-03-12 19:05:51.521 | 169.46 | Acetylene
-- E-3 | 2008-03-12 19:05:51.555 | 169.12 | Hydrogen cyanide

-- Exploring INMS Readings
-- High and Low sensitivity Counts
-- Let’s look at the relative density of each chemical compound
-- C1COUNTS => “high sensitivity counts”
-- C2COUNTS => “low sensitivity counts”
-- with density counters
select
  inms.chem_data.name,
  sum(high_counts) as high_counts,
  sum(low_counts) as low_counts
from flybys
inner join inms.readings on
  time_stamp >= flybys.window_start
  and time_stamp <= flybys.window_end
inner join inms.chem_data on peak = mass_per_charge
where flybys.id = 4
group by inms.chem_data.name, flybys.speed
order by high_counts desc;
-- name | high_counts | low_counts
-- Molecular Hydrogen | 5904 | 16
-- Carbon Monoxide | 2972 | 6
-- Ethylene | 2348 | 4

-- Querying Counts by Source
-- hmm, the high reading of molecular hydrogen is misleading
-- adding `source` to the query will tell us about the analysis of each of
-- these chemicals and how it was performed
-- The SOURCE is the Ion source used for this measurement
  --  osi = Open Source Ion
  -- csn = Closed Source Neutral
  -- osnb = Open Source Neutral Beam
  -- osnt = Open Source Neutral Thermal
-- closed source is used to measure non-reactive neutrals like CH4 and N2
-- open source measures positive ion species
-- results of flyby E-3 show that molecular hydrogen (H2) found using closed
-- source mode
-- E-3 and Molecular Hydrogen
select
  inms.chem_data.name,
  source,
  sum(high_counts) as high_counts,
  sum(low_counts) as low_counts
from flybys
inner join inms.readings on
  time_stamp >= flybys.window_start
  and time_stamp <= flybys.window_end
inner join inms.chem_data on peak = mass_per_charge
where flybys.id = 4
group by inms.chem_data.name, source
order by high_counts desc;
-- name | source | high_counts | low_counts
-- Molecular Hydrogen | csn | 5892 | 8
-- Carbon Monoxide | csn | 2944 | 5
-- Ethylene | csn | 2328 | 4

-- problem using closed source mode is that the chamber itself can react with
-- certain species and throw off the results

-- Open Source Molecular Hydrogen
-- open source mode lets plume material directly into the instrument
-- Looking at E-21 with the flyby name
-- flyby | name | source | high_counts | low_counts
-- E-21 | Water | csn | 590842 | 151
-- E-21 | Molecular Hydrogen | csn | 115172 | 40
-- E-21 | Molecular Hydrogen | osnb | 3872 | 4

-- One last query
-- are we alone in the universe?
-- such a small query for such a big question...
select
  flybys.name as flyby,
  inms.chem_data.name,
  source,
  sum(high_counts) as high_counts,
  sum(low_counts) as low_counts
from flybys
inner join inms.readings on
  time_stamp >= flybys.window_start
  and time_stamp <= flybys.window_end
inner join inms.chem_data on peak = mass_per_charge
where flybys.targeted == true
and formula in ('H2', 'CH4', 'CO2', 'H2O')
group by flybys.id, flybys.name, inms.chem_data.name, source
order by flybys.id;
-- the chemical thumbprint of life
-- flyby | name | source | high_counts | low_counts
-- E-2 | Methane | csn | 76 | 4
-- E-2 | Methane | osi | 4 | 0
-- E-2 | Water | csn | 101 | 0
-- this proves nothing, but the data suggests that Cassini detected everything
-- but life
