#include "postgres.h"

#include <math.h>
#include "access/tupdesc.h"
#include "access/htup_details.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/datetime.h"
#include "utils/dynahash.h"
#include "utils/timestamp.h"
#include "pg_comment_stats.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

static char *worker_name = "pgcs_worker";

static char *extension_name = "pg_comment_stats";

static HTAB *string_to_id = NULL;

static HTAB *id_to_string = NULL;

static GlobalInfo *global_variables = NULL;

static volatile sig_atomic_t got_sigterm = false;

static int stat_time_interval;
static int buffer_size_mb;
static char* excluded_keys = NULL;

void _PG_init(void);
void _PG_fini(void);

static pgsk_counters_hook_type prev_pgsk_counters_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void pgcs_shmem_startup(void);
void pgcs_register_bgworker(void);
static void get_composite_key_by_query(char*, pgcsCompositeKey*, bool*);
static int get_index_of_comment_key(char*);
static int get_id_from_string(char*);
static Datum pgcs_internal_get_stats_time_interval(TimestampTz, TimestampTz, FunctionCallInfo);
void pgcs_add(void*, void*);
void pgcs_on_delete(void*, void*);
static uint64_t pgcs_find_optimal_memory_split(uint64_t, uint64_t, uint64_t*, uint64_t*, uint64_t*, int*, int*);
void pg_comment_stats_main(Datum main_arg);
void pgcs_store_aggregated_counters(pgskCounters*, char*, int, pgskStoreKind);

static int
get_random_int(void)
{
    int x;

    x = (int) (random() & 0xFFFF) << 16;
    x |= (int) (random() & 0xFFFF);
    return x;
}

static void
pg_comment_stats_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

void
_PG_init(void) {
    int64 buffer_size;
    if (!process_shared_preload_libraries_in_progress) {
        elog(ERROR, "This module can only be loaded via shared_preload_libraries");
        return;
    }

    pgcs_register_bgworker();
    DefineCustomIntVariable("pg_comment_stats.buffer_size",
                            "Max amount of shared memory (in megabytes), that can be allocated",
                            "Default of 20, max of 5000",
                            &buffer_size_mb,
                            20,
                            min_buffer_size_mb,
                            max_buffer_size_mb,
                            PGC_SUSET,
                            GUC_UNIT_MB | GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("pg_comment_stats.stat_time_interval",
                            "Duration of stat collecting interval",
                            "Default of 6000s, max of 360000s",
                            &stat_time_interval,
                            6000,
                            min_time_interval,
                            max_time_interval,
                            PGC_SUSET,
                            GUC_UNIT_S | GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("pg_comment_stats.excluded_keys",
                               "Excluded keys separated by ','",
                               NULL,
                               &excluded_keys,
                               NULL,
                               PGC_POSTMASTER,
                               0,    /* no flags required */
                               NULL,
                               NULL,
                               NULL);

    buffer_size = buffer_size_mb * 1024 * 1024;

    EmitWarningsOnPlaceholders("pg_comment_stats");
    RequestAddinShmemSpace(buffer_size);
#if PG_VERSION_NUM >= 90500
    RequestNamedLWLockTranche("pg_comment_stats", 1);
#else
    RequestAddinLWLocks(1);
#endif

    /* Install hook */
    prev_pgsk_counters_hook = pgsk_counters_hook;
    pgsk_counters_hook = pgcs_store_aggregated_counters;
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = pgcs_shmem_startup;
}

void
_PG_fini(void)
{
    /* uninstall hook */
    shmem_startup_hook = prev_shmem_startup_hook;
    pgsk_counters_hook = prev_pgsk_counters_hook;
}

static void
pgcs_shmem_startup(void)
{
    bool		        found;
    HASHCTL		        info;
    bool                found_global_info;
    int                 id;
    int                 items_count;
    pgcsStringFromId    *stringFromId;
    uint64_t            total_shmem;
    uint64_t            pgtb_size;
    int                 global_var_const_size;
    uint64_t            string_to_id_htab_item;
    uint64_t            id_to_string_htab_item;
    char                excluded_keys_copy[max_parameters_count * max_parameter_length];
    char*               excluded_key;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    /* Create or attach to the shared memory state */
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    global_var_const_size = sizeof(GlobalInfo);
    string_to_id_htab_item = (sizeof(char) * max_parameter_length + sizeof(pgcsIdFromString));
    id_to_string_htab_item = (sizeof(int) + sizeof(pgcsStringFromId));

    total_shmem = buffer_size_mb * 1024 * 1024;
    pgtb_size = pgcs_find_optimal_memory_split(0,
                                               total_shmem,
                                               &string_to_id_htab_item,
                                               &id_to_string_htab_item,
                                               &total_shmem,
                                               &global_var_const_size,
                                               &items_count);

    pgtb_init(extension_name,
              &pgcs_add,
              &pgcs_on_delete,
              (int)(stat_time_interval / buckets_count + 0.5),
              pgtb_size,
              sizeof(pgcsBucketItem),
              sizeof(pgskCounters));

    global_variables = ShmemInitStruct("pg_comment_stats global_variables",
                                       sizeof(GlobalInfo),
                                       &found_global_info);

    global_variables->bucket_duration = (int)(stat_time_interval / buckets_count + 0.5);
    global_variables->items_count = items_count;
    elog(LOG, "pgcs: Max count of unique strings: %d", global_variables->items_count);

    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(char) * max_parameter_length;
    info.entrysize = sizeof(pgcsIdFromString);
    string_to_id = ShmemInitHash("pg_comment_stats string_to_id_htab",
                                 items_count, items_count,
                                 &info,
                                 HASH_ELEM | HASH_BLOBS);

    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int);
    info.entrysize = sizeof(pgcsStringFromId);

    id_to_string = ShmemInitHash("pg_comment_stats id_to_string_htab",
                                 items_count, items_count,
                                 &info,
                                 HASH_ELEM | HASH_BLOBS);

    id = comment_value_not_specified;
    stringFromId = hash_search(id_to_string, (void *) &id, HASH_ENTER, &found);
    global_variables->currents_strings_count = 1;
    stringFromId->id = id;
    memset(stringFromId->string, '\0', sizeof(stringFromId->string));

    memset(&global_variables->excluded_keys, '\0', sizeof(global_variables->excluded_keys));
    global_variables->excluded_keys_count = 0;

    if (excluded_keys != NULL) {
        /* make sure that variable not too big */
        memset(&excluded_keys_copy, '\0', sizeof(excluded_keys_copy));
        strlcpy(&excluded_keys_copy[0], excluded_keys, max_parameters_count * max_parameter_length);
        excluded_key = strtok(excluded_keys_copy, ",");
        while (excluded_key != NULL) {
            strlcpy(&global_variables->excluded_keys[global_variables->excluded_keys_count][0], excluded_key, max_parameter_length - 1);
            global_variables->excluded_keys_count += 1;

            excluded_key = strtok(NULL, " ");
        }
    }
    LWLockRelease(AddinShmemInitLock);
}

static bool
is_key_excluded(char *key) {
    int i;
    for (i = 0; i < global_variables->excluded_keys_count; ++i) {
        if (strcmp(key, global_variables->excluded_keys[i]) == 0)
            return true;
    }
    return false;
}

static int
get_index_of_comment_key(char *key) {
    int i;
    if (global_variables == NULL) {
        return comment_key_not_specified;
    }

    if (is_key_excluded(key))
        return comment_key_not_specified;

    for (i = 0; i < global_variables->keys_count; ++i) {
        if (strcmp(key, global_variables->commentKeys[i]) == 0)
            return i;
    }
    if (global_variables->keys_count == max_parameters_count) {
        global_variables->keys_overflow = true;
        return comment_key_not_specified;
    }
    strcpy(global_variables->commentKeys[global_variables->keys_count++], key);
    return global_variables->keys_count - 1;
}

static void
get_composite_key_by_query(char *query, pgcsCompositeKey *compositeKey, bool *is_comment_exist) {
    char query_copy[(max_parameter_length + 2) * max_parameters_count * 2];
    char *end_of_comment;
    char *start_of_comment;
    int comment_length;
    int len;
    int i;
    char end_of_key[2] = ":\0";
    int key_index = -1;
    char *query_prefix;
    int value_id;

    start_of_comment = strstr(query, "/*");
    end_of_comment = strstr(query, "*/");

    *is_comment_exist = false;
    if (start_of_comment == NULL || end_of_comment == NULL) {
        compositeKey = NULL;
        return;
    }

    start_of_comment += 3;
    comment_length = end_of_comment - start_of_comment;
    memset(&query_copy, '\0', sizeof(query_copy));
    strlcpy(query_copy, start_of_comment, comment_length + 1);
    query_copy[comment_length] = '\0';
    query_prefix = strtok(query_copy, " ");

    for (i = 0; i < max_parameters_count; ++i) {
        compositeKey->keyValues[i] = comment_value_not_specified;
    }
    while (query_prefix != NULL) {
        len = strlen(query_prefix);
        if (strcmp(query_prefix + len - 1, end_of_key) == 0 && key_index == -1) {
            /* it's key */
            query_prefix[len - 1] = '\0';
            key_index = get_index_of_comment_key(query_prefix);
        } else {
            /* it's value */
            if (key_index == comment_key_not_specified) {
                query_prefix = strtok(NULL, " ");
                continue;
            }
            value_id = get_id_from_string(query_prefix);
            if (value_id == -1) {
                return;
            }
            *is_comment_exist = true;
            compositeKey->keyValues[key_index] = value_id;
            key_index = -1;
        }

        query_prefix = strtok(NULL, " ");
    }
    if (key_index != -1)
        elog(WARNING, "pg_comment_stats: incorrect comment");
}

static uint64_t
pgcs_find_optimal_items_count(uint64_t left_bound,
                              uint64_t right_bound,
                              uint64_t* string_to_id_htab_item,
                              uint64_t* id_to_string_htab_item,
                              uint64_t* total_size) {
    uint64_t string_to_id_htab_size;
    uint64_t id_to_string_htab_size;
    uint64_t middle;

    middle = (right_bound + left_bound) / 2;
    string_to_id_htab_size = hash_estimate_size(middle, *string_to_id_htab_item);
    id_to_string_htab_size = hash_estimate_size(middle, *id_to_string_htab_item);

    if (left_bound + 1 == right_bound) {
        return left_bound;
    }

    if (string_to_id_htab_size + id_to_string_htab_size > *total_size) {
        return pgcs_find_optimal_items_count(left_bound,
                                             middle,
                                             string_to_id_htab_item,
                                             id_to_string_htab_item,
                                             total_size);
    } else {
        return pgcs_find_optimal_items_count(middle,
                                             right_bound,
                                             string_to_id_htab_item,
                                             id_to_string_htab_item,
                                             total_size);
    }
}

static uint64_t
pgcs_find_optimal_memory_split(uint64_t left_bound,
                               uint64_t right_bound,
                               uint64_t* string_to_id_htab_item,
                               uint64_t* id_to_string_htab_item,
                               uint64_t* total_size,
                               int* global_var_const_size,
                               int* pgcs_items) {
    int pgcs_max_items_count;
    uint64_t middle;
    uint64_t pgcs_size;
    int pgtb_items;

    middle = (right_bound + left_bound) / 2;
    pgcs_size = *total_size - middle;
    pgcs_max_items_count = pgcs_size / (*string_to_id_htab_item + *id_to_string_htab_item);

    pgtb_items = pgtb_get_items_count(middle,
                         sizeof(pgcsBucketItem),
                         sizeof(pgskCounters));

    *pgcs_items = pgcs_find_optimal_items_count(0,
                                               pgcs_max_items_count,
                                               string_to_id_htab_item,
                                               id_to_string_htab_item,
                                               &pgcs_size);

    if (left_bound + 1 == right_bound) {
        return left_bound;
    }

    if (pgtb_items < *pgcs_items) {
        return pgcs_find_optimal_memory_split(middle,
                                              right_bound,
                                              string_to_id_htab_item,
                                              id_to_string_htab_item,
                                              total_size,
                                              global_var_const_size,
                                              pgcs_items);
    } else {
        return pgcs_find_optimal_memory_split(left_bound,
                                              middle,
                                              string_to_id_htab_item,
                                              id_to_string_htab_item,
                                              total_size,
                                              global_var_const_size,
                                              pgcs_items);
    }
}

void
pgcs_store_aggregated_counters(pgskCounters* counters, char* query_string, int level, pgskStoreKind kind) {
    pgcsBucketItem key;
    pgskCounters additional_counters;
    bool is_comment_exist;
    char query[(max_parameter_length + 1) * max_parameters_count];

    if (global_variables == NULL) {
        return;
    }

    if (prev_pgsk_counters_hook)
        prev_pgsk_counters_hook(counters, query_string, level, kind);

    /* Now track only top-level statements */
    /* TODO: maybe add support of other levels? */
    if (level != 0)
        return;

    memset(&key.compositeKey, 0, sizeof(pgcsCompositeKey));
    strlcpy(query, query_string, sizeof(query));
    query[sizeof(query) - 1] = '\0';
    is_comment_exist = true;
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    get_composite_key_by_query((char*)&query, &key.compositeKey, &is_comment_exist);
    if (!is_comment_exist) {
        LWLockRelease(&global_variables->lock);
        return;
    }
    key.database = MyDatabaseId;
    key.user = GetUserId();

    additional_counters.usage = 1;
    additional_counters.utime = counters->utime;
    additional_counters.stime = counters->stime;

#ifdef HAVE_GETRUSAGE
    additional_counters.minflts = counters->minflts;
    additional_counters.majflts = counters->majflts;
    additional_counters.nswaps = counters->nswaps;
    additional_counters.reads = counters->reads;
    additional_counters.writes = counters->writes;
    additional_counters.msgsnds = counters->msgsnds;
    additional_counters.msgrcvs = counters->msgrcvs;
    additional_counters.nsignals = counters->nsignals;
    additional_counters.nvcsws = counters->nvcsws;
    additional_counters.nivcsws = counters->nivcsws;
#endif
    LWLockRelease(&global_variables->lock);
    pgtb_put(extension_name, &key, &additional_counters);
}

void pgcs_add(void* value, void* anotherValue) {
    pgskCounters* counters;
    pgskCounters* another_counters;

    counters = (pgskCounters*) value;
    another_counters = (pgskCounters*) anotherValue;

    counters->usage += another_counters->usage;
    counters->utime += another_counters->utime;
    counters->stime += another_counters->stime;
    counters->minflts += another_counters->minflts;
    counters->majflts += another_counters->majflts;
    counters->nswaps += another_counters->nswaps;
    counters->reads += another_counters->reads;
    counters->writes += another_counters->writes;
    counters->msgsnds += another_counters->msgsnds;
    counters->msgrcvs += another_counters->msgrcvs;
    counters->nsignals += another_counters->nsignals;
    counters->nvcsws += another_counters->nvcsws;
    counters->nivcsws += another_counters->nivcsws;
}

void
pgcs_register_bgworker() {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_PostmasterStart;
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
    sprintf(worker.bgw_library_name, "pg_comment_stats");
    sprintf(worker.bgw_function_name, "pg_comment_stats_main");
    /* Wait 10 seconds for restart after crash */
    worker.bgw_restart_time = 10;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;
    RegisterBackgroundWorker(&worker);
}

static int
get_id_from_string(char *string_pointer) {
    char string[max_parameter_length];
    pgcsIdFromString *idFromString;
    pgcsStringFromId *stringFromId;
    int id;
    bool found;
    if (!string_to_id || !id_to_string)
        return comment_value_not_specified;
    if (strlen(string_pointer) >= max_parameter_length) {
        elog(WARNING, "pg stat kcache: Comment value %s too long to store. Max length is %d",
             string_pointer,
             max_parameter_length);
        return comment_value_not_specified;
    }
    memset(&string, '\0', max_parameter_length);
    strcpy(string, string_pointer);
    idFromString = hash_search(string_to_id, (void *) &string, HASH_FIND, &found);
    if (!found) {
        found = true;
        /* Generate id that not used yet. */
        while (found) {
            id = get_random_int();
            stringFromId = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
        }

        if (global_variables->currents_strings_count < global_variables->items_count) {
            stringFromId = hash_search(id_to_string, (void *) &id, HASH_ENTER, &found);
            global_variables->currents_strings_count += 1;
            stringFromId->id = id;
            memset(stringFromId->string, '\0', max_parameter_length);
            strcpy(stringFromId->string, string);
            stringFromId->counter = 0;

            idFromString = hash_search(string_to_id, (void *) &string, HASH_ENTER, &found);
            memset(idFromString->string, '\0', max_parameter_length);
            strcpy(idFromString->string, string);
            idFromString->id = id;
        } else {
            if (!global_variables->max_strings_count_achieved) {
                elog(WARNING,
                     "pgcs: Can't handle request. No more memory for save strings are available. "
                     "Current max count of unique strings = %d."
                     "Decide to tune pg_comment_stats.buffer_size", global_variables->items_count);
            }
            global_variables->max_strings_count_achieved = true;
            global_variables->strings_overflow_by += 1;
            return -1;
        }
    }
    stringFromId = hash_search(id_to_string, (void *) &idFromString->id, HASH_FIND, &found);
    stringFromId->counter++;

    return idFromString->id;
}

static void
get_string_from_id(int id, char* string) {
    pgcsStringFromId *stringFromId;
    bool found;
    if (!string_to_id || !id_to_string) {
        return;
    }
    stringFromId = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
    strlcpy(string, stringFromId->string, max_parameter_length);
}

void pgcs_on_delete(void* key, void* value) {
    pgcsBucketItem* item;
    pgskCounters* counters;
    pgcsStringFromId *string_struct;
    int id;
    int i;
    bool found;

    if (global_variables == NULL)
        return;

    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    item = (pgcsBucketItem*) key;
    counters = (pgskCounters*) value;

    for (i = 0; i < global_variables->keys_count; ++i) {
        id = item->compositeKey.keyValues[i];
        if (id == comment_value_not_specified) {
            continue;
        }
        string_struct = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
        string_struct->counter -= (int)(counters->usage + 0.5);
        if (string_struct->counter == 0) {
            hash_search(id_to_string, (void *) &id, HASH_REMOVE, &found);
            hash_search(string_to_id, (void *) string_struct->string, HASH_REMOVE, &found);
            global_variables->currents_strings_count -= 1;
        }
    }
    LWLockRelease(&global_variables->lock);

}

static void
pgcs_init() {
    LWLockAcquire(&global_variables->reset_lock, LW_EXCLUSIVE);
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    memset(&global_variables->commentKeys, '\0', sizeof(global_variables->commentKeys));
    global_variables->max_strings_count_achieved = false;
    global_variables->keys_count = 0;
    global_variables->strings_overflow_by = 0;
    global_variables->keys_overflow = false;
    LWLockRelease(&global_variables->lock);
    LWLockRelease(&global_variables->reset_lock);
}

static void
pgcs_update_info() {
    if (global_variables == NULL) {
        return;
    }
    if (global_variables->max_strings_count_achieved) {
        elog(WARNING, "pg_comment_stats: Too many unique strings. Overflow by %d (%f%%)",
             global_variables->strings_overflow_by, (double)global_variables->strings_overflow_by / global_variables->items_count);
    }
    if (global_variables->keys_overflow) {
        elog(WARNING, "pg_stat_kcache: Can not store more than %d parameters", max_parameters_count);
    }
    global_variables->keys_overflow = false;
    global_variables->strings_overflow_by = 0;
    global_variables->max_strings_count_achieved = false;
}

void
pg_comment_stats_main(Datum main_arg) {
    TimestampTz timestamp;
    int64 wait_microsec;
    /* Register functions for SIGTERM management */
    pqsignal(SIGTERM, pg_comment_stats_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();
    LWLockInitialize(&global_variables->lock, LWLockNewTrancheId());
    LWLockInitialize(&global_variables->reset_lock, LWLockNewTrancheId());

    pgcs_init();
    wait_microsec = global_variables->bucket_duration * 1e6;
    while (!got_sigterm) {
        int rc;

        /* Wait necessary amount of time */
        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, wait_microsec / 1000, PG_WAIT_EXTENSION);
        /* Emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);
        /* Process signals */
        timestamp = GetCurrentTimestamp();
        ResetLatch(&MyProc->procLatch);
        if (got_sigterm) {
            /* Simply exit */
            elog(DEBUG1, "bgworker pg_comment_stats signal: processed SIGTERM");
            proc_exit(0);
        }
        /* Main work happens here */
        pgcs_update_info();
        LWLockAcquire(&global_variables->reset_lock, LW_EXCLUSIVE);
        pgtb_tick(extension_name);
        LWLockRelease(&global_variables->reset_lock);
        wait_microsec = (int64) global_variables->bucket_duration * 1e6 - (GetCurrentTimestamp() - timestamp);
        if (wait_microsec < 0)
            wait_microsec = 0;
    }

    /* No problems, so clean exit */
    proc_exit(0);
}

static char *escape_json_string(const char *str) {
    size_t str_len = strlen(str);
    size_t pstr_len = 2 * (str_len + 1);
    int i, j = 0;
    char *pstr = palloc(pstr_len);
    for (i = 0; i < str_len; i++) {
        char ch = str[i];
        switch (ch) {
            case '\\':
                pstr[j++] = '\\';
                pstr[j++] = '\\';
                break;
            case '"':
                pstr[j++] = '\\';
                pstr[j++] = '"';
                break;
            case '\n':
                pstr[j++] = '\\';
                pstr[j++] = 'n';
                break;
            case '\r':
                pstr[j++] = '\\';
                pstr[j++] = 'r';
                break;
            case '\t':
                pstr[j++] = '\\';
                pstr[j++] = 't';
                break;
            case '\b':
                pstr[j++] = '\\';
                pstr[j++] = 'b';
                break;
            case '\f':
                pstr[j++] = '\\';
                pstr[j++] = 'f';
                break;
            default:
                pstr[j++] = ch;
        }
        if (j == pstr_len) break;
    }
    pstr[j++] = '\0';
    return pstr;
}


PG_FUNCTION_INFO_V1(pgcs_get_stats);
PG_FUNCTION_INFO_V1(pgcs_get_stats_time_interval);

Datum
get_jsonb_datum_from_key(pgcsCompositeKey *compositeKey) {
    StringInfoData strbuf;
    int i;
    char component[max_parameter_length];
    char *escaped_component;
    const char* const_component;
    bool some_value_stored;
    int keys_count;
    char commentKeys[max_parameters_count][max_parameter_length];

    some_value_stored = false;
    initStringInfo(&strbuf);
    appendStringInfoChar(&strbuf, '{');

    memcpy(&commentKeys, &global_variables->commentKeys, sizeof(commentKeys));
    keys_count = global_variables->keys_count;

    for (i = 0; i < keys_count; ++i) {
        if (compositeKey->keyValues[i] == comment_value_not_specified) {
            continue;
        }
        memset(&component, '\0', sizeof(component));
        if (i > 0 && some_value_stored)
            appendStringInfoChar(&strbuf, ',');
        get_string_from_id(compositeKey->keyValues[i], (char*)&component);
        const_component = component;
        escaped_component = escape_json_string(const_component);

        appendStringInfo(&strbuf, "\"%s\":\"%s\"", (char*)&commentKeys[i], escaped_component);
        some_value_stored = true;
        pfree(escaped_component);
    }
    appendStringInfoChar(&strbuf, '}');
    appendStringInfoChar(&strbuf, '\0');

    return DirectFunctionCall1(jsonb_in, CStringGetDatum(strbuf.data));
}

Datum
pgcs_get_stats(PG_FUNCTION_ARGS) {

    TimestampTz timestamp_left;
    TimestampTz timestamp_right;

    timestamp_right = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), global_variables->bucket_duration * 100000);
    timestamp_left = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), global_variables->bucket_duration * (- buckets_count * 100000));

    return pgcs_internal_get_stats_time_interval(timestamp_left, timestamp_right, fcinfo);
}

Datum
pgcs_get_stats_time_interval(PG_FUNCTION_ARGS) {
    TimestampTz timestamp_left;
    TimestampTz timestamp_right;

    timestamp_left = PG_GETARG_TIMESTAMP(0);
    timestamp_right = PG_GETARG_TIMESTAMP(1);

    return pgcs_internal_get_stats_time_interval(timestamp_left, timestamp_right, fcinfo);
}

/* TODO: Currently it's not possible to remove key from stats. Maybe add this possibility? */
static Datum
pgcs_internal_get_stats_time_interval(TimestampTz timestamp_left, TimestampTz timestamp_right, FunctionCallInfo fcinfo) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    int message_id;
    pgcsBucketItem key;
    pgskCounters value;
    uint64 reads, writes;
    char timestamp_left_s[max_parameter_length];
    char timestamp_right_s[max_parameter_length];

    int items_count;
    void* result_ptr;
    int length;
    int i;

    Datum values[PG_STAT_KCACHE_COLS + 1];
    bool nulls[PG_STAT_KCACHE_COLS + 1];

    /* Shmem structs not ready yet */
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_comment_stats must be loaded via shared_preload_libraries")));
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("return type must be a row type")));
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, 0, sizeof(nulls));
    MemSet(&key, 0, sizeof(pgcsBucketItem));
    LWLockAcquire(&global_variables->reset_lock, LW_EXCLUSIVE);
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);

    items_count = global_variables->items_count;
    result_ptr = (void*) palloc(items_count * (sizeof(pgcsBucketItem) + sizeof(pgskCounters)));
    memset(result_ptr, 0, items_count * (sizeof(pgcsBucketItem) + sizeof(pgskCounters)));
    pgtb_get_stats_time_interval(extension_name, &timestamp_left, &timestamp_right, result_ptr, &length);

    strcpy(timestamp_left_s, timestamptz_to_str(timestamp_left));
    strcpy(timestamp_right_s, timestamptz_to_str(timestamp_right));

    elog(NOTICE, "pgcs: Show stats from '%s' to '%s'", timestamp_left_s, timestamp_right_s);

    /* put to tuplestore and clear bucket index -1 */
    for (message_id = 0; message_id < length; ++message_id) {
        memcpy(&key,
               (char*)result_ptr + (sizeof(pgcsBucketItem) + sizeof(pgskCounters)) * message_id,
               sizeof(pgcsBucketItem));
        memcpy(&value,
               (char*)result_ptr + (sizeof(pgcsBucketItem) + sizeof(pgskCounters)) * message_id + sizeof(pgcsBucketItem),
               sizeof(pgskCounters));

        MemSet(nulls, false, sizeof(nulls));
        i = 0;
        values[i++] = get_jsonb_datum_from_key(&key.compositeKey);

        values[i++] = Int64GetDatum(value.usage);
        values[i++] = ObjectIdGetDatum(key.user);
        values[i++] = ObjectIdGetDatum(key.database);

#ifdef HAVE_GETRUSAGE
        reads = value.reads * RUSAGE_BLOCK_SIZE;
        writes = value.writes * RUSAGE_BLOCK_SIZE;
        values[i++] = Int64GetDatumFast(reads);
        values[i++] = Int64GetDatumFast(writes);
#else
        nulls[i++] = true; /* reads */
        nulls[i++] = true; /* writes */
#endif
        values[i++] = Float8GetDatumFast(value.utime);
        values[i++] = Float8GetDatumFast(value.stime);

#ifdef HAVE_GETRUSAGE
        values[i++] = Int64GetDatumFast(value.minflts);
        values[i++] = Int64GetDatumFast(value.majflts);
        values[i++] = Int64GetDatumFast(value.nswaps);
        values[i++] = Int64GetDatumFast(value.msgsnds);
        values[i++] = Int64GetDatumFast(value.msgrcvs);
        values[i++] = Int64GetDatumFast(value.nsignals);
        values[i++] = Int64GetDatumFast(value.nvcsws);
        values[i++] = Int64GetDatumFast(value.nivcsws);

#else
        nulls[i++] = true; /* minflts */
        nulls[i++] = true; /* majflts */
        nulls[i++] = true; /* nswaps */
        nulls[i++] = true; /* msgsnds */
        nulls[i++] = true; /* msgrcvs */
        nulls[i++] = true; /* nsignals */
        nulls[i++] = true; /* nvcsws */
        nulls[i++] = true; /* nivcsws */
#endif
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    pfree(result_ptr);
    LWLockRelease(&global_variables->lock);
    /* return the tuplestore */
    tuplestore_donestoring(tupstore);
    LWLockRelease(&global_variables->reset_lock);
    return (Datum) 0;
}

PG_FUNCTION_INFO_V1(pgcs_exclude_key);

Datum
pgcs_exclude_key(PG_FUNCTION_ARGS) {
    char key_copy[max_parameter_length];
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_comment_stats must be loaded via shared_preload_libraries")));
    LWLockAcquire(&global_variables->reset_lock, LW_EXCLUSIVE);
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    if (global_variables->excluded_keys_count == max_parameters_count) {
        elog(WARNING, "pgcs: Can't exclude more than %d keys. You can drop existing keys by 'pgcs_reset_excluded_keys'",
             max_parameters_count);
    }
    else {
        memset(&key_copy, 0, sizeof(key_copy));
        strlcpy((char*)&key_copy, PG_GETARG_CSTRING(0), sizeof(key_copy) - 1);
        if (is_key_excluded((char*)&key_copy)) {
            elog(WARNING, "pgcs: Key already excluded.");
        } else {
            strlcpy(&global_variables->excluded_keys[global_variables->excluded_keys_count][0], PG_GETARG_CSTRING(0), max_parameter_length - 1);
            global_variables->excluded_keys_count++;
        }
    }
    LWLockRelease(&global_variables->lock);
    LWLockRelease(&global_variables->reset_lock);

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pgcs_get_excluded_keys);

Datum
pgcs_get_excluded_keys(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    Datum values[1];
    bool nulls[1];
    int i;

    /* Shmem structs not ready yet */
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_comment_stats must be loaded via shared_preload_libraries")));
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_SCALAR)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("return type must be a scalar type")));
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;

    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, 0, sizeof(nulls));
#if PG_VERSION_NUM >= 120000
    tupdesc = CreateTemplateTupleDesc(1);
#else
    tupdesc = CreateTemplateTupleDesc(1, false);
#endif
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "excluded_key",
                       TEXTOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(&global_variables->reset_lock, LW_EXCLUSIVE);
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    for (i = 0; i < global_variables->excluded_keys_count; ++i) {
        values[0] = CStringGetTextDatum(global_variables->excluded_keys[i]);
        nulls[0] = false;
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    LWLockRelease(&global_variables->lock);
    LWLockRelease(&global_variables->reset_lock);

    tuplestore_donestoring(tupstore);
    return (Datum) 0;
}

PG_FUNCTION_INFO_V1(pgcs_get_buffer_stats);

Datum
pgcs_get_buffer_stats(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    Datum values[BUFFER_STATS_COUNT];
    bool nulls[BUFFER_STATS_COUNT];

    /* Shmem structs not ready yet */
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_comment_stats must be loaded via shared_preload_libraries")));
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("return type must be a row type")));
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;

    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, 0, sizeof(nulls));
#if PG_VERSION_NUM >= 120000
    tupdesc = CreateTemplateTupleDesc(BUFFER_STATS_COUNT);
#else
    tupdesc = CreateTemplateTupleDesc(BUFFER_STATS_COUNT, false);
#endif
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "saved_strings_count",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "available_strings_count",
                       INT4OID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(&global_variables->reset_lock, LW_EXCLUSIVE);
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    values[0] = Int32GetDatum(global_variables->currents_strings_count);
    values[1] = Int32GetDatum(global_variables->items_count);

    LWLockRelease(&global_variables->lock);
    LWLockRelease(&global_variables->reset_lock);
    tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    tuplestore_donestoring(tupstore);
    return (Datum) 0;
}

PG_FUNCTION_INFO_V1(pgcs_reset_excluded_keys);

Datum
pgcs_reset_excluded_keys(PG_FUNCTION_ARGS) {
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_comment_stats must be loaded via shared_preload_libraries")));

    LWLockAcquire(&global_variables->reset_lock, LW_EXCLUSIVE);
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    memset(&global_variables->excluded_keys, '\0', sizeof(global_variables->excluded_keys));
    global_variables->excluded_keys_count = 0;
    LWLockRelease(&global_variables->lock);
    LWLockRelease(&global_variables->reset_lock);
    PG_RETURN_VOID();
}
