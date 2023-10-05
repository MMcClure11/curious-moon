/* Find the flybys. */

/* To run the queries, while in the `curious_data` directory, first setup the
* db with `make clean && make`. Then to start querying, open a connection in
* your terminal to the database with `psql enceladus`. Copy these queries and
* run them in the terminal to see the results.  */

/* Join the targets table to know what planet or moon the mission focused on.
 Get the title to determine what was the mission.*/
select targets.description as target,
time_stamp,
title
from events
inner join targets on target_id=targets.id;

/* Returns list of targets, time stamps and titles where the title includes
 flyby or fly by.*/
select targets.description as target,
time_stamp,
title
from events
inner join targets on target_id=targets.id
where title like '%flyby%'
or title like '%fly by%';

/* Returns list of targets, time stamps and titles where the title includes
 flyby or fly by but case insensitive.*/
select targets.description as target,
time_stamp,
title
from events
inner join targets on target_id=targets.id
where title ilike '%flyby%'
or title ilike '%fly by%';

/* Use regex to take advantage of the pattern for Titan. T0 flyby, T3 flyby,
* etc. Check all titles that start with T and have one or more numbers
* following that end with the term flyby. ~* is a comparison operator that
* tells Postgres this is a case-insensitive match operation. This returns 121
* rows. */
select targets.description as target,
time_stamp,
title
from events
inner join targets on target_id=targets.id
where title ~* '^T\d.*? flyby'
order by time_stamp;

/* How to determine the count: */
select count(*)
from events
inner join targets on target_id=targets.id
where title ~* '^T\d.*? flyby';
/* count
-------
   121 */

/* Check to see if the regex is too restrictive. Expand the filter to allow any
 word. 397 rows!*/
select targets.description as target,
time_stamp,
title
from events
inner join targets on target_id=targets.id
where title ~* '^T[A-Z0-9_].*? flyby'
order by time_stamp;

/*
Add a few expressions to get more data to work with. Add target information,
cast the time_stamp to a date. Before running the query, run `\x` to turn on
the expanded view which returns results with the following format:

-[ RECORD 1 ]-------------------------------------------------------------------
target     | Phoebe
event      | Phoebe targeted flyby
time_stamp | 2004-06-11 17:33:37
date       | 2004-06-11
title      | Phoebe targeted flyby
-[ RECORD 2 ]-------------------------------------------------------------------
target     | Titan
event      | MAG Titan observation
time_stamp | 2004-07-02 05:30:21
date       | 2004-07-02
title      | T0 Flyby
*/

select targets.description as target,
event_types.description as event,
time_stamp,
time_stamp::date as date,
title
from events
left join targets on target_id=targets.id
left join event_types on event_type_id=event_types.id
where title ilike '%flyby%'
or title ilike '%fly by%'
order by time_stamp;

/* The Enceladus pass was February 17, 2005. But it isn't in the list using the
 above query. Query the source table and restrict the date to see what's
 happening.

-[ RECORD 1 ]----------------------------------------------------------
target | Enceladus
title  | Enceladus
date   | 17-Feb-05
-[ RECORD 2 ]----------------------------------------------------------
target | Enceladus
title  | Enceladus FP1 and FP3 maps, high spectral resoluti
date   | 17-Feb-05
-[ RECORD 3 ]----------------------------------------------------------
target | Enceladus
title  | ICYLON:  Icy Satellite Longitude / Phase Coverage
date   | 17-Feb-05
-[ RECORD 4 ]----------------------------------------------------------
target | Saturn
title  | MAPS Survey
date   | 17-Feb-05
-[ RECORD 5 ]----------------------------------------------------------
target | Saturn
title  | Obtain wideband examples of lightning whistlers
date   | 17-Feb-05

Yay! The first fly by of Enceladus is recorded! The code in the book doesn't
seem quite right. It has an extra `order by time_stamp` after the first one
which is incorrect syntax. I think the correct way would be to say `order by
start_time_utc::date, time_stamp`, but that field doesn't exist on the table we
are querying: `ERROR:  column "time_stamp" does not exist`.

https://commandprompt.com/education/how-to-sort-multiple-columns-using-order-by-clause-in-postgresql/
*/
select target, title, date
from import.master_plan
where start_time_utc::date = '2005-02-17'
order by start_time_utc::date;

/* Restrict the query to a single day via casting. Turns the timestamp info a single day only instead of a day with hours, minutes, seconds… Cast `timestamptz` to a `date` */
select targets.description as target,
events.time_stamp,
event_types.description as event
from events
inner join event_types on event_types.id = events.event_type_id
inner join targets on targets.id = events.target_id
where events.time_stamp::date='2005-02-17'
order by events.time_stamp;
/*
      target       |     time_stamp      |                           event
 Enceladus         | 2005-02-17 03:00:29 | Enceladus closest approach observation
…
*/

/* Restrict the results so that Enceladus is the only target. Chain `and` clauses to further restric a query. */
select targets.description as target,
events.time_stamp,
event_types.description as event
from events
inner join event_types on event_types.id = events.event_type_id
inner join targets on targets.id = events.target_id
where events.time_stamp::date='2005-02-17'
and targets.description = 'enceladus'
order by events.time_stamp;
/* 
 target | time_stamp | event 
--------+------------+-------
(0 rows)

No results. Postgres is case sensitive.
String comparison queries are fragile & slow. The Postgres engine has to do a lot of work to compare strings.
Better to find the key value and use that in the query.
Look in `targets` table for primary key that defines `Enceladus`target.
*/
select * from targets where description = 'Enceladus';
/* description | id 
-------------+----
 Enceladus   | 28
(1 row) 

use the id as a filter instead
*/

select targets.description as target,
events.time_stamp,
event_types.description as event
from events
inner join event_types on event_types.id = events.event_type_id
inner join targets on targets.id = events.target_id
where events.time_stamp::date='2005-02-17'
and targets.id = 28
order by events.time_stamp;
/*
target   |     time_stamp      |                 event                  
-----------+---------------------+----------------------------------------
 Enceladus | 2005-02-17 00:00:29 | Enceladus
 Enceladus | 2005-02-17 00:15:29 | CIRS FP1 integration / FP3 map
(24 rows)

Less fragile, faster.
*/

/*
Sargeable & Non-Sargeable queries
Sargeable => Search ARGument ABLE. Can the string query be optimized? Optimizable.
Non-Sargeable => Not Optimizable.
Based on the nature of the query itself.

select * from events
where description like 'closest%';

No wild card before the string meaning the query planner could optimize the search by adding an index to the description field.

Sequential scan => when Postgres has to search every row in the table and run a string comparison

Try to use sargeable queries when possible, especially in produstion.
*/

/* Using a View to Make Querying Easier */
