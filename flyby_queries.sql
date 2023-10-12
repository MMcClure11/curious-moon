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

/*
Views are stored snippets of SQL.
Created by passing in a `select` statement to a `create view` statement.
*/

drop view if exists enceladus_events;
create view enceladus_events as
select
events.time_stamp,
events.time_stamp::date as date,
event_types.description as event
from events
inner join event_types
on event_types.id = events.event_type_id
where target_id=28
order by time_stamp;

/* Then we can query for a given day using the date directly. */
select * from enceladus_events where date='2005-02-17'
/* or */
select * from enceladus_events where date='2/17/2005'

/*
Results: 24
     time_stamp      |    date    |                 event
---------------------+------------+----------------------------------------
 2005-02-17 00:00:29 | 2005-02-17 | Enceladus
 2005-02-17 00:15:29 | 2005-02-17 | CIRS FP1 integration / FP3 map
 2005-02-17 00:15:29 | 2005-02-17 | Icy satellite longitudinal coverage
 2005-02-17 01:30:29 | 2005-02-17 | Icy satellite longitudinal coverage
 2005-02-17 01:30:29 | 2005-02-17 | Enceladus
 2005-02-17 01:30:29 | 2005-02-17 | Enceladus
…
*/

/* Fix up the view to get some more detail */

drop view if exists enceladus_events;
create view enceladus_events as
select
events.id,
events.title,
events.description,
events.time_stamp,
events.time_stamp::date as date,
event_types.description as event
from events
inner join event_types
on event_types.id = events.event_type_id
where target_id=28
order by time_stamp;

/*
The query only runs when you `select` information from the view, so creating it takes no time at all.
This returns results in a very difficult to read manner. Let’s try using HTML to read the query results
In your psql session, redirect STDOUT to an HTML file

enceladus=# \H
Output format is html.
enceladus=# \o feb_2015_flyby.html
enceladus=# select id, time_stamp, title, description from enceladus_events where date='2005-02-17'::date;

This created the `feb_2015_flyby.html` that is in this current working directory. You can open it via your shell with:

curious_data on  a-bent-field [!?]
❮ open feb_2015_flyby.html

which pops it open in your browser.

Interestingly enough, the two `ids` mentioned in the book, 14654 and 41467 I don’t see in my output.
Ah, but if I look for the description or title, I can find it. I guess the IDs don’t line up for some reason.
*/

/*
Next, lets look at March

enceladus=# \H
Output format is html.
enceladus=# \o mar_2015_flyby.html
enceladus=# select id, time_stamp, title, description from enceladus_events where date='2005-03-09'::date;
*/

/*
Need to make searching 60_000 records easier and faster.
Full-text indexing: Tweak a body of text & index it, priortizing useful terms and deprioritizing noise.
Use the `to_tsvector` function to create an index that can be searched over.
[Controlling Text Search](https://www.postgresql.org/docs/current/textsearch-controls.html)
*/

drop view if exists enceladus_events;
create view enceladus_events as
select
events.id,
events.title,
events.description,
events.time_stamp,
events.time_stamp::date as date,
event_types.description as event,
to_tsvector(events.description) as search
from events
inner join event_types
on event_types.id = events.event_type_id
where target_id=28
order by time_stamp;

-- find the thermal results
select id, date, title from enceladus_events
where date between '2005-02-01'::date and '2005-02-28'::date
and search @@ to_tsquery('thermal');
-- @@ is specific to search queries and means “show me the matches”
-- [Text Search Functions & Operators](https://www.postgresql.org/docs/current/functions-textsearch.html)

/*
  id   |    date    |                             title
-------+------------+----------------------------------------------------------------
 19368 | 2005-02-16 | Enceladus rider (FP3 integration)
 58873 | 2005-02-16 | RSS Enceladus Gravity Thermal Stabilization
 19366 | 2005-02-16 | Enceladus rider (FP3 integration)
 18688 | 2005-02-17 | Enceladus FP1 and FP3 maps, high spectral resoluti
 14636 | 2005-02-17 | Enceladus rider (FP1,FP3 high spectral resolution integration)
 30309 | 2005-02-17 | Enceladus rider (FP1,FP3 coverage)
 58893 | 2005-02-17 | RSS Enceladus Gravity Thermal Stabilization
 41488 | 2005-02-17 | Enceladus rider (FP3 0.5 cm-1 res. integration)
(8 rows)
*/

select title, teams.description from events
inner join teams on teams.id=team_id
where time_stamp::date='2005-03-09'
and target_id=28;

/*
                       title                        | description
----------------------------------------------------+-------------
 Enceladus Orbit Xing 0.00<lat< 0.50 deg 3.85... 4. | CDA
 Enceladus Orbit Xing 0.00<lat< 0.50 deg 3.85... 4. | CDA
 Dust envelope around Enceladus                     | CDA
 Enceladus closest approach observations            | RPWS
 Enceladus targeted                                 | VIMS
(57 rows)
…

What were the teams doing with the second flyby ^.

Which team had the most time? Let’s find out using an aggregate query, sorted by time.
*/
select count(1) as activity, teams.description from events
inner join teams on teams.id=team_id
where time_stamp::date='2005-03-09'
and target_id=28
group by teams.description
order by activity desc;
/*
 activity | description
----------+-------------
       14 | CIRS
       12 | UVIS
       12 | VIMS
        7 | ISS
        3 | CDA
        3 | INMS
        2 | MAG
        1 | RADAR
        1 | RPWS
        1 | MIMI
        1 | CAPS
(11 rows)
*/

-- Add indexing with multiple columns

drop view if exists enceladus_events;
create view enceladus_events as
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

select id, title from enceladus_events where search @@ to_tsquery('closest');
/*
Need to find better flyby dates, use the enceladus_events view to account for all 23.
Each flyby has the term closest in the title, so we added that to the view’s index.
We got 25 instead of 23, so we are picking up some extra.
  id   |                      title
-------+-------------------------------------------------
 14422 | Enceladus closest approach observations
 14423 | Enceladus closest approach observations
 14421 | Enceladus closest approach observations
 14424 | Enceladus closest approach observations
  9777 | Drag CIRS FOVs across disk at closest approach.
 14409 | Enceladus closest approach observations
…
(25 rows)

** Materialized View**

A view is a projection of data from a source. Nothing is stored anywhere. Every
time we query a view we actually query the underlying tables through it. You
can only index a view if it exists on disk. That’s why they are called
`materialized` views.
*/

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

select id, date, title from enceladus_events
where search @@ to_tsquery('closest');
/*
Indexes work like they do for books. Find the term you are looking for
alphabetically in the index then go to the given page. We can use a GIN index.
This must be for a materialized view. A regular view can’t be indexed since
it’s not stored anywhere.

  id   |    date    |                      title
-------+------------+-------------------------------------------------
 14422 | 2005-02-17 | Enceladus closest approach observations
 14423 | 2005-03-09 | Enceladus closest approach observations
 14421 | 2005-07-14 | Enceladus closest approach observations
 14424 | 2008-03-12 | Enceladus closest approach observations
  9777 | 2008-03-12 | Drag CIRS FOVs across disk at closest approach.
 14409 | 2008-08-11 | Enceladus closest approach observations
 14413 | 2008-10-09 | Enceladus closest approach observations
 14405 | 2008-10-31 | Enceladus closest approach observations
 14414 | 2009-11-02 | Enceladus closest approach observations
 14417 | 2009-11-02 | Enceladus closest approach observations
…
(25 rows)

There are 2 closest approaches on November 02.
*/

select (time_stamp at time zone 'UTC'),
title
from events
where (time_stamp at time zone 'UTC')::date='2009-11-02'
order by time_stamp;

-- Even converting to UTC doesn’t solve the issue. It may be a data entry mistake.
