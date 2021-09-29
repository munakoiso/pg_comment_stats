# pg_comment_stats

Saves stats about queries using keys from comments and counters from pg_stat_kcache.

Extension requires pg_stat_statements and pg_stat_kcache extension to be installed.

Compiling
---------

The module can be built using the standard PGXS infrastructure. For this to
work, the ``pg_config`` program must be available in your $PATH. 
Also, [pg_time_buffer](https://github.com/munakoiso/pg_time_buffer) 
and header from [pg_stat_kcache](https://github.com/powa-team/pg_stat_kcache) is required to compile.

Instruction to
install follows:
```
 git clone https://github.com/powa-team/pg_stat_kcache.git
 git clone https://github.com/munakoiso/pg_time_buffer.git
 git clone https://github.com/munakoiso/pg_comment_stats.git
 cp pg_stat_kcache/pg_stat_kcache.h pg_comment_stats/
 cp pg_time_buffer/* pg_comment_stats/
 cd pg_comment_stats
 make
 make install
```

Setup
---
You must add the module to ``shared_preload_libraries`` in your ``postgresql.conf``.


Configuration
---
The following GUCs can be configured, in ``postgresql.conf``:

- *pg_comment_stats.buffer_size* - amount of shared memory (in mb), that will be allocated by extension.
- *pg_comment_stats.stat_time_interval* - interval of time (in seconds), during which stats will be stored in the buffer. After this interval, each stat will be removed from the buffer.
- *pg_comment_stats.excluded_keys* - list of excluded keys (for example "first_key,second_key")

Usage
-----


Add comments like this to your queries:

```> /* a: 1 b: qwerty*/ some_query;```

Select stats about specified keys from all buffer:
```
> select * from pgcs_get_stats();
NOTICE:  00000: pgcs: Show stats from '2021-09-29 15:33:03.97336+03' to '2021-09-29 15:33:13.785069+03'
LOCATION:  pgcs_internal_get_stats_time_interval, pg_comment_stats.c:848
-[ RECORD 1 ]+---------------------------
comment_keys | {"a": "1", "b": "qwerty"}
query_count  | 1
userid       | 10
dbid         | 14409
reads        | 0
writes       | 0
user_time    | 5.700000000000019e-05
system_time  | 0
minflts      | 7
majflts      | 0
nswaps       | 0
msgsnds      | 0
msgrcvs      | 0
nsignals     | 0
nvcsws       | 0
nivcsws      | 0

Time: 3.649 ms
```

Or from specific time interval (with accuracy 1% of stat_time_interval):

```
> select * from pgcs_get_stats_time_interval(now() - '30 s'::interval, now());
NOTICE:  00000: pgcs: Show stats from '2021-09-29 15:36:08.98296+03' to '2021-09-29 15:36:39.567835+03'
LOCATION:  pgcs_internal_get_stats_time_interval, pg_comment_stats.c:848
-[ RECORD 1 ]+---------------------------
comment_keys | {"a": "1", "b": "qwerty"}
query_count  | 1
userid       | 10
dbid         | 14409
reads        | 0
writes       | 0
user_time    | 0
system_time  | 4.0000000000000105e-05
minflts      | 0
majflts      | 0
nswaps       | 0
msgsnds      | 0
msgrcvs      | 0
nsignals     | 0
nvcsws       | 0
nivcsws      | 0

Time: 1.408 m
```

If needed, you can exclude some keys:

```
> select pgcs_exclude_key('c');
 pgcs_exclude_key
------------------

(1 row)

Time: 0.344 ms

> /* a: 1 c: hmm*/ select 1;
 ?column?
----------
        1
(1 row)

Time: 0.209 ms

> select * from pgcs_get_stats() limit 1;
NOTICE:  00000: pgcs: Show stats from '2021-09-29 15:37:54.982949+03' to '2021-09-29 15:39:36.513423+03'
LOCATION:  pgcs_internal_get_stats_time_interval, pg_comment_stats.c:848
-[ RECORD 1 ]+----------------------
comment_keys | {"a": "1"}
query_count  | 1
userid       | 10
dbid         | 14409
reads        | 0
writes       | 0
user_time    | 6.000000000000363e-06
system_time  | 1.299999999999999e-05
minflts      | 0
majflts      | 0
nswaps       | 0
msgsnds      | 0
msgrcvs      | 0
nsignals     | 0
nvcsws       | 0
nivcsws      | 0

Time: 1.125 ms
```

Select or reset excluded keys:

```
> select * from pgcs_get_excluded_keys();
 excluded_key
--------------
 c
(1 row)

Time: 0.283 ms
> select * from pgcs_reset_excluded_keys();
 pgcs_reset_excluded_keys
--------------------------

(1 row)

Time: 0.285 ms
> select * from pgcs_get_excluded_keys();
 excluded_key
--------------
(0 rows)

Time: 0.188 ms
```