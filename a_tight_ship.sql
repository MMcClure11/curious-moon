-- A TIGHT SHIP

/*
The Data-First Mindset

PostgreSQL is almost always the clear, distinct, and correct choice.
Data is your business. Sifting through records or logs to answer specific
business questions becomes much easier with the data-first mindset.
Ensure your application generates data that not only answers, but anticipates
the boss or clients questions about their business.
Good data is good observation, free from bias (as much as possible), the data
is only as good as the ability to interrogate and understand it.
Storing application data is a solved problem.

ACID - Atomic, Consistent, Isolated, Durable
- if you get an ack that data has been written, it’s on disk, guaranteed

LOCKING AND THE MVCC - Multi-Version Concurrency Control
Most database systems will lock records when they’re part of a write
transaction in order to avoid serving “dirty data” (data that’s old and has
been updated behind the scenes). To prevent this each record in the transaction
is locked until the entire transaction completes and reading is blocked and put
into a queue until the data is unlocked. This can be a problem if you need to
update a flag on each user, you have a million or more users, and none of them
can access their info while waiting for all the transactions to complete.

Postgres locks data as well, but only as a fallback measure.
It uses MVCC - Multi-Version Concurrency Control: every bit of data that is
part of a transaction is “lifted” into virtual memory and all changes happen
there. Only when the transaction is complete is the entire change written to
disk in one operation. You cannot write to a record that’s currently in a
transaction. The write will be queued until the entire transaction is
completed. Prevents read locks for the most part.

ENTERPRISE FEATURES READY TO GO
“Create index concurrently”
Partitioning Data - allows you to scale your db easily by separating data into
physical partitions

FRIENDLY
*/

select '2000-01-01 00:00:00'::timestamptz;
/*
Postgres can parse this, assumes current time zone with 0 milliseconds. (I
think mine is different because I have UTC set to default for my postgres db.)

select '2000-01-01 00:00:00'::timestamptz;
      timestamptz
------------------------
 2000-01-01 00:00:00+00
(1 row)
*/

select '2000-01-01 00:00:00'::timestamptz + '1';
/*
enceladus=# select '2000-01-01 00:00:00'::timestamptz + '1';
        ?column?
------------------------
 2000-01-01 00:00:01+00
(1 row)

Postgres can determine intervals based on strings values. The default interval
is seconds. Proven by the fact that our timestamp has a second added to it.
*/

select '2000-01-01 00:00:00'::timestamptz + 1;
/*
enceladus=# select '2000-01-01 00:00:00'::timestamptz + 1;
ERROR:  operator does not exist: timestamp with time zone + integer
LINE 1: select '2000-01-01 00:00:00'::timestamptz + 1;
                                                  ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.

Postgres insists on clarity, and offers hints on how to be clearer.
*/
