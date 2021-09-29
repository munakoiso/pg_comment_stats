CREATE FUNCTION pgcs_get_stats(
    OUT comment_keys    jsonb,
    OUT query_count     integer,
    OUT userid          oid,
    OUT dbid            oid,
    OUT reads           bigint,             /* total reads, in bytes */
    OUT writes          bigint,             /* total writes, in bytes */
    OUT user_time       double precision,   /* total user CPU time used */
    OUT system_time     double precision,   /* total system CPU time used */
    OUT minflts         bigint,             /* total page reclaims (soft page faults) */
    OUT majflts         bigint,             /* total page faults (hard page faults) */
    OUT nswaps          bigint,             /* total swaps */
    OUT msgsnds         bigint,             /* total IPC messages sent */
    OUT msgrcvs         bigint,             /* total IPC messages received */
    OUT nsignals        bigint,             /* total signals received */
    OUT nvcsws          bigint,             /* total voluntary context switches */
    OUT nivcsws         bigint              /* total involuntary context switches */
)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pgcs_get_stats'
    LANGUAGE C STRICT;

CREATE FUNCTION pgcs_get_stats_time_interval(
    start_ts            timestamptz,
    stop_ts             timestamptz,
    OUT comment_keys    jsonb,
    OUT query_count     integer,
    OUT userid          oid,
    OUT dbid            oid,
    OUT reads           bigint,             /* total reads, in bytes */
    OUT writes          bigint,             /* total writes, in bytes */
    OUT user_time       double precision,   /* total user CPU time used */
    OUT system_time     double precision,   /* total system CPU time used */
    OUT minflts         bigint,             /* total page reclaims (soft page faults) */
    OUT majflts         bigint,             /* total page faults (hard page faults) */
    OUT nswaps          bigint,             /* total swaps */
    OUT msgsnds         bigint,             /* total IPC messages sent */
    OUT msgrcvs         bigint,             /* total IPC messages received */
    OUT nsignals        bigint,             /* total signals received */
    OUT nvcsws          bigint,             /* total voluntary context switches */
    OUT nivcsws         bigint              /* total involuntary context switches */
)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pgcs_get_stats_time_interval'
    LANGUAGE C STRICT;

CREATE FUNCTION pgcs_exclude_key(
    exclude_string  cstring
)
    RETURNS void
AS 'MODULE_PATHNAME', 'pgcs_exclude_key'
    LANGUAGE C STRICT;

CREATE FUNCTION pgcs_get_excluded_keys(
    OUT excluded_key text
)
AS 'MODULE_PATHNAME', 'pgcs_get_excluded_keys'
    LANGUAGE C STRICT;

CREATE FUNCTION pgcs_reset_excluded_keys()
    RETURNS void
AS 'MODULE_PATHNAME', 'pgcs_reset_excluded_keys'
    LANGUAGE C STRICT;

CREATE FUNCTION pgcs_get_buffer_stats(
    OUT saved_strings_count     integer,
    OUT available_strings_count integer
) STRICT
AS 'MODULE_PATHNAME', 'pgcs_get_buffer_stats'
    LANGUAGE C;
