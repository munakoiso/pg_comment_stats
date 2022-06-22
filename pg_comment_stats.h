#ifndef PGSK_AGGREGATED_STATS
#define PGSK_AGGREGATED_STATS

#include "postgres.h"

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/jsonb.h"
#include "executor/execdesc.h"
#include "nodes/execnodes.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"

#include "pg_stat_kcache.h"
#include "pg_time_buffer.h"
#include "pg_comment_stats_constants.h"

typedef struct pgcsStringFromId {
    int id;
    char string[max_parameter_length];
    int counter;
} pgcsStringFromId;

typedef struct pgcsIdFromString {
    char string[max_parameter_length];
    int id;
} pgcsIdFromString;

typedef struct pgcsCompositeKey {
    int keyValues[max_parameters_count];
} pgcsCompositeKey;

typedef struct pgcsBucketItem {
    pgcsCompositeKey compositeKey;
    Oid database;
    Oid user;
} pgcsBucketItem;

Datum get_jsonb_datum_from_key(pgcsCompositeKey *);

typedef struct global_info {
    char commentKeys[max_parameters_count][max_parameter_length];
    char excluded_keys[max_parameters_count][max_parameter_length];
    int excluded_keys_count;
    int keys_count;
    int currents_strings_count;
    int items_count;
    int bucket_duration;
    int strings_overflow_by;
    bool keys_overflow;
    LWLock lock;
    LWLock reset_lock;
    bool max_strings_count_achieved;
    pgcsBucketItem buckets[FLEXIBLE_ARRAY_MEMBER];
} GlobalInfo;

void pgcs_register_bgworker(void);
extern void pgtb_init(const char*,
               void (*add)(void*, void*),
               void (*on_delete)(void*, void*),
               int,
               uint64_t,
               uint64_t,
               uint64_t
               );
extern bool pgtb_put(const char*, void*, void*);
extern void pgtb_tick(const char*);
extern void pgtb_get_stats(const char*, void*, int*, TimestampTz*, TimestampTz*);
extern void pgtb_get_stats_time_interval(const char*, TimestampTz*, TimestampTz*, void*, int*);
extern int pgtb_get_items_count(uint64_t, uint64_t, uint64_t);

bool pgcs_enabled;
#endif
