#include "rock_evict.h"
// #include "rock.h"
#include "rock_hash.h"
#include "rock_write.h"

/* For rockEvictDictType, each db has just one instance.
 * For each key which can be evicted to RocksDB, it store a key and value.
 * The key is redis db key, shared with db->dict, so do not need key destructor.
 * The value is the time.
 * 
 * NOTE: The rockEvictDictType is not the only object for eviction to RocksDB,
 *       we have one more for each db, it is rock hash.
 *       Check rock_hash.h and rock_hash.c for more details.
 * 
 * The hash will add to the rockHashDictType,
 * key is db redis key, shared with db->dict, so do not need key destructor.
 * NOTE:
 *     After the key added to rockHashDictType, it will not
 *     be deleted from the rockHashDictType 
 *     (even the field number drop to the threshold or server.hash_max_rock_entries change) 
 *     until the key is deleted from redis db.
 * value is pointer to a dict with valid lru (which has not been evicted to RocksDB). 
 *     Check fieldLruDictType for more info.
 */
int dictExpandAllowed(size_t moreMem, double usedRatio);    // declaration in server.c
dictType rockEvictDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL,                       /* val destructor */
    dictExpandAllowed           /* allow to expand */
};

/* API for server initianization of db->rock_hash for each redisDB, 
 * like db->dict, db->expires */
dict* init_rock_evict_dict()
{
    return dictCreate(&rockEvictDictType, NULL);
}

/* When redis server start and finish loading RDB/AOF,
 * we need to add the matched key to rock evict.
 * 
 * NOTE: 1. We need exclude those keys not approciatte, like stream or shared object.
 *       2. We need exclude those keys in rock hash, so init_rock_hash_before_enter_event_loop()
 *          must be executed before init_rock_evict_before_enter_event_loop()
 */
void init_rock_evict_before_enter_event_loop()
{
    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;

        dictIterator *di = dictGetIterator(db->dict);
        dictEntry *de;
        while ((de = dictNext(di)))
        {
            const sds key = dictGetKey(de);
            const robj *o = dictGetVal(de);

            if (!is_rock_type(o))
                continue;

            if (is_shared_value(o))
                continue;

            if (is_in_rock_hash(i, key))
                continue;

            serverAssert(dictAdd(db->rock_evict, key, NULL) == DICT_OK);
        }
        dictReleaseIterator(di);
    }
}

/* When a key has been added to a redis db, we need 
 * check whether it is OK for add to rock evict and add it.
 * NOTE: for rock evcit dictionary, the key is the internal key in redis db->dict
 */
void on_db_add_key_for_rock_evict(const int dbid, const sds internal_key)
{
    redisDb *db = server.db + dbid;

    dictEntry *de = dictFind(db->dict, internal_key);
    serverAssert(de);

    robj *o = dictGetVal(de);

    #if defined RED_ROCK_DEBUG
    serverAssert(dictGetKey(de) == internal_key);
    serverAssert(!is_in_rock_hash(dbid, internal_key));
    serverAssert(!is_rock_value(o));
    #endif

    if (!is_rock_type(o))
        return;

    if (is_shared_value(o))
        return;

    serverAssert(dictAdd(db->rock_evict, internal_key, NULL) == DICT_OK);
}

/* Before a key deleted from a redis db, 
 * We need to delete it from rock evict if it exists.
 */
void on_db_del_key_for_rock_evict(const int dbid, const sds key)
{
    redisDb *db = server.db + dbid;

    dictEntry *de = dictFind(db->dict, key);
    serverAssert(de);
    sds internal_key = dictGetKey(de);

#if defined RED_ROCK_DEBUG
    robj *o = dictGetVal(de);

    if (is_rock_value(o))
    {
        serverAssert(dictFind(db->rock_evict, internal_key) == NULL);
        return;
    }

    if (!is_rock_type(o))
    {
        serverAssert(dictFind(db->rock_evict, internal_key) == NULL);
        return;
    }

    if (is_shared_value(o))
    {
        serverAssert(dictFind(db->rock_evict, internal_key) == NULL);
        return;
    }

    if (is_in_rock_hash(dbid, key))
    {
        serverAssert(dictFind(db->rock_evict, internal_key) == NULL);
        return;
    }
#endif

    // NOTE: could exist in rock_evict or not
    dictDelete(db->rock_evict, internal_key);
}

/* After a key is overwritten in redis db, it will call here.
 *       e.g., set k v  then set k 1. This example will change the value to a shared object.
 * The new_o is the replaced object for the redis_key in db.
 */
void on_db_overwrite_key_for_rock_evict(const int dbid, const sds key, const robj *new_o)
{
    redisDb *db = server.db + dbid;
    // NOTE: could exist in rock evict or not
    dictDelete(db->rock_evict, key);       

    dictEntry *de = dictFind(db->dict, key);
    serverAssert(de);

    #if defined RED_ROCK_DEBUG
    serverAssert(!is_rock_value(new_o));
    #endif

    if (!is_rock_type(new_o))
        return;

    if (is_shared_value(new_o))
        return;

    const sds internal_key = dictGetKey(de);

    // NOTE: because on_overwrite_key_from_db_for_rock_hash()
    //       call before on_db_overwrite_key_for_rock_evict()
    if (is_in_rock_hash(dbid, internal_key))
        return;

    serverAssert(dictAdd(db->rock_evict, internal_key, NULL) == DICT_OK);
}

/* After a key of hash added to rock hash,
 * we need delete it from rock_evcit.
 */
void on_transfer_to_rock_hash(const int dbid, const sds internal_key)
{
    redisDb *db = server.db + dbid;

    serverAssert(dictDelete(db->rock_evict, internal_key) == DICT_OK);
}

/* Because we use LRU directly from the object value in redis db,
 * so we need not to update anything relating to lru.
 */
void on_db_visit_key_for_rock_evict(const int dbid, const sds key)
{
    UNUSED(dbid);
    UNUSED(key);
}

/* When rock_write.c already set one whole keys's value to rock value,
 * it needs delete the key from rock evict.
 */
void on_rockval_key_for_rock_evict(const int dbid, const sds internal_key)
{
    redisDb *db = server.db + dbid;

    serverAssert(dictDelete(db->rock_evict, internal_key) == DICT_OK);
}

/* When rock_read.c already recover a whole key from RocksDB or ring buffer, 
 * it needs to add itself to rock hash
 * 
 * NOTE: We must add the internal key of redis db.
 */
void on_recover_key_for_rock_evict(const int dbid, const sds internal_key)
{
    redisDb *db = server.db + dbid;

#if defined RED_ROCK_DEBUG
    dictEntry *de = dictFind(db->dict, internal_key);
    serverAssert(de);
    serverAssert(dictGetKey(de) == internal_key);
#endif

    serverAssert(dictAdd(db->rock_evict, internal_key, NULL) == DICT_OK);
}

/* When flushdb or flushalldb, it will empty the db(s).
 * and we need reclaim the rock evict in the db.
 * if dbnum == -1, it means all db
 */
void on_empty_db_for_rock_evict(const int dbnum)
{
    serverAssert(dbnum == -1 || (dbnum >= 0 && dbnum < server.dbnum));

    int start = dbnum;
    if (dbnum == -1)
        start = 0;

    int end = dbnum + 1;
    if (dbnum == -1)
        end = server.dbnum;

    for (int dbid = start; dbid < end; ++dbid)
    {
        redisDb *db = server.db + dbid;
        dict *rock_evict = db->rock_evict;
        dictEmpty(rock_evict, NULL);
    }
}

/*                                              */
/* The following is for eviction pool operation */
/*                                              */

#define EVPOOL_SIZE             16      // like Redis eviction pool size
#define EVPOOL_CACHED_SDS_SIZE  255

struct evictKeyPoolEntry 
{
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
};

struct evictHashPoolEntry 
{
    unsigned long long idle;
    sds field;
    sds cached_field;       /* Cached SDS object for field. */
    sds hash_key;
    sds cached_hash_key;    /* Cached SDS object for redis key. */
    int dbid;
};

/* The two gloabl pool */
static struct evictKeyPoolEntry *evict_key_pool = NULL;
static struct evictHashPoolEntry *evict_hash_pool = NULL;

/* Create a new eviction pool for db key of rock evict */
static void evict_key_pool_alloc(void) 
{
    struct evictKeyPoolEntry *ep = 
           zmalloc(sizeof(struct evictKeyPoolEntry) * EVPOOL_SIZE);

    for (int i = 0; i < EVPOOL_SIZE; ++i) 
    {
        ep[i].idle = 0;
        ep[i].key = NULL;
        ep[i].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[i].dbid = 0;
    }

    evict_key_pool = ep;
}

/* Create a new eviction pool for rock hash. */
static void evict_hash_pool_alloc(void) {
    struct evictHashPoolEntry *ep = 
           zmalloc(sizeof(struct evictHashPoolEntry) * EVPOOL_SIZE);

    for (int i = 0; i < EVPOOL_SIZE; ++i) 
    {
        ep[i].idle = 0;
        ep[i].field = NULL;
        ep[i].hash_key = NULL;
        ep[i].cached_field = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[i].cached_hash_key = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[i].dbid = 0;
    }

    evict_hash_pool = ep;
}

/* Call when server start to init the eviction pool 
 */
void evict_pool_init()
{
    evict_key_pool_alloc();
    evict_hash_pool_alloc();
}

/* Check evcit.c evictionPoolPopulate() for more details.
 * We use the similiar algorithm for eviction so it needs to populate
 * the eviction pool and use some tricks for optimization of cached sds.
 * 
 * Return the number of populated key.
 */
#define SAMPLE_KEY_NUMBER           5       // like Redis sample number
static size_t evict_key_pool_populate(const int dbid)
{
    // first, sample some keys from rock_evict
    redisDb *db = server.db + dbid;
    if (dictSize(db->rock_evict) == 0)
        return 0;

    dictEntry* sample_des[SAMPLE_KEY_NUMBER];
    const unsigned int count = dictGetSomeKeys(db->rock_evict, sample_des, SAMPLE_KEY_NUMBER);
    if (count == 0)
        return 0;       // NOTE dictGetSomeKeys could return zero if dict size is very slow

    size_t insert_cnt = 0;
    struct evictKeyPoolEntry *pool = evict_key_pool;

    for (int j = 0; j < (int)count; j++) 
    {
        dictEntry *de = sample_des[j];
        const sds key = dictGetKey(de);

        dictEntry *de_db = dictFind(db->dict, key);
        serverAssert(de_db);
        robj *o = dictGetVal(de_db);
        
        unsigned long long idle = server.maxmemory_policy & MAXMEMORY_FLAG_LFU ? 
                                  255 - (o->lru & 255) : estimateObjectIdleTime(o);

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time. */
        int k = 0;
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle) k++;

        if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) 
        {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
        } 
        else if (k < EVPOOL_SIZE && pool[k].key == NULL) 
        {
            /* Inserting into empty position. No setup needed before insert. */
        } 
        else 
        {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            if (pool[EVPOOL_SIZE-1].key == NULL) 
            {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                sds cached = pool[EVPOOL_SIZE-1].cached;
                memmove(pool+k+1, pool+k,
                    sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                pool[k].cached = cached;
            } 
            else 
            {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                sds cached = pool[0].cached; /* Save SDS before overwriting. */
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                memmove(pool, pool+1, sizeof(pool[0])*k);
                pool[k].cached = cached;
            }
            ++insert_cnt;
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        size_t klen = sdslen(key);
        if (klen > EVPOOL_CACHED_SDS_SIZE) 
        {
            pool[k].key = sdsdup(key);
        } 
        else 
        {
            memcpy(pool[k].cached,key,klen+1);
            sdssetlen(pool[k].cached,klen);
            pool[k].key = pool[k].cached;
        }

        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }

    return insert_cnt;
}
#undef SAMPLE_KEY_NUMBER

/* Check the above evict_key_pool_populate() for more details.
 * The algorithm is similiar. 
 */
#define SAMPLE_HASH_KEY_NUMBER     5
#define SAMPLE_HASH_FIELD_NUMBER    5
static size_t evict_hash_pool_populate(const int dbid)
{
    // first, sample some fields from rock_hash
    // NOTE: rock hash has two levels of dict, so we pick only one hash key
    //       from the first level dict (i.e., rock_hash), then SAMPLE_HASH_FIELD_NUMBER 
    //       from the second level dict (i.e., lrus)      
    redisDb *db = server.db + dbid;
    if (dictSize(db->rock_hash) == 0)
        return 0;

    dictEntry* sample_key_des[SAMPLE_HASH_KEY_NUMBER];
    const unsigned int key_count = dictGetSomeKeys(db->rock_hash, sample_key_des, SAMPLE_HASH_KEY_NUMBER);
    if (key_count == 0)
        return 0;       // NOTE dictGetSomeKeys could return zero if dict size is very slow

    // NOTE: We sample hash key, but we only use only one hash key which has the most lrus
    int max_hash_index = 0;
    size_t max_lru_size = dictSize((dict*)dictGetVal(sample_key_des[0]));
    for (unsigned int i = 1; i < key_count; ++i)
    {
        dict *current_lrus = dictGetVal(sample_key_des[i]);
        size_t current_lru_size = dictSize(current_lrus);
        if (current_lru_size > max_lru_size)
        {
            max_hash_index = i;
            max_lru_size = current_lru_size;
        }
    } 

    sds hash_key = dictGetKey(sample_key_des[max_hash_index]);
    dict *lrus = dictGetVal(sample_key_des[max_hash_index]);

    // NOTE: lrus could be empty
    //       e.g., a hash key in rock_hash then all fields have benn evicted
    //             NOTE: the hash key with empty lrus can not be deleted 
    //                   because it indicates some fields in RoocksDB 
    //                   so the eviction can not deal it as a whole key.                
    if (dictSize(lrus) == 0)
        return 0;   

    dictEntry* sample_field_des[SAMPLE_HASH_FIELD_NUMBER];
    const unsigned int field_count = dictGetSomeKeys(lrus, sample_field_des, SAMPLE_HASH_FIELD_NUMBER);
    if (field_count == 0)
        return 0;   // NOTE dictGetSomeKeys could return zero if dict size is very slow

    size_t insert_cnt = 0;
    struct evictHashPoolEntry *pool = evict_hash_pool;

    for (int j = 0; j < (int)field_count; j++) 
    {
        dictEntry *de_field = sample_field_des[j];
        const sds field = dictGetKey(de_field);
        const unsigned int lru = (uint64_t)dictGetVal(de_field);

        #if defined RED_ROCK_DEBUG
        dictEntry *de_db = dictFind(db->dict, hash_key);
        serverAssert(de_db);
        robj *o = dictGetVal(de_db);
        serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);
        dict *dict_hash = (dict*)o->ptr;
        serverAssert(dictSize(lrus) <= dictSize(dict_hash));
        #endif
        
        unsigned long long idle = server.maxmemory_policy & MAXMEMORY_FLAG_LFU ? 
                                  255 - (lru & 255) : lru;

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time. */
        int k = 0;
        while (k < EVPOOL_SIZE &&
               pool[k].field &&
               pool[k].idle < idle) k++;

        if (k == 0 && pool[EVPOOL_SIZE-1].field != NULL) 
        {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
        } 
        else if (k < EVPOOL_SIZE && pool[k].field == NULL) 
        {
            /* Inserting into empty position. No setup needed before insert. */
        } 
        else 
        {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            if (pool[EVPOOL_SIZE-1].field == NULL) 
            {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                sds cached_field = pool[EVPOOL_SIZE-1].cached_field;
                sds cached_hash_key = pool[EVPOOL_SIZE-1].cached_hash_key;
                memmove(pool+k+1, pool+k,
                    sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                pool[k].cached_field = cached_field;
                pool[k].cached_hash_key = cached_hash_key;
            } 
            else 
            {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                sds cached_field = pool[0].cached_field; /* Save SDS before overwriting. */
                sds cached_hash_key = pool[0].cached_hash_key;
                if (pool[0].field != pool[0].cached_field) sdsfree(pool[0].field);
                if (pool[0].hash_key != pool[0].cached_hash_key) sdsfree(pool[0].hash_key);
                memmove(pool, pool+1, sizeof(pool[0])*k);
                pool[k].cached_field = cached_field;
                pool[k].cached_hash_key = cached_hash_key;
            }
            ++insert_cnt;
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        size_t field_klen = sdslen(field);
        if (field_klen > EVPOOL_CACHED_SDS_SIZE) 
        {
            pool[k].field = sdsdup(field);
        } 
        else 
        {
            memcpy(pool[k].cached_field, field, field_klen+1);
            sdssetlen(pool[k].cached_field, field_klen);
            pool[k].field = pool[k].cached_field;
        }
        size_t hash_key_klen = sdslen(hash_key);
        if (hash_key_klen > EVPOOL_CACHED_SDS_SIZE)
        {
            pool[k].hash_key = sdsdup(hash_key);
        }
        else
        {
            memcpy(pool[k].cached_hash_key, hash_key, hash_key_klen+1);
            sdssetlen(pool[k].cached_hash_key, hash_key_klen);
            pool[k].hash_key = pool[k].cached_hash_key;
        }

        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }

    return insert_cnt;
}
#undef SAMPLE_HASH_FIELD_NUMBER

/* From the key pool, find the right key to be evicted.
 * Because the pool has previous values, so we need to take care of it.
 * And we also clear the pool for scanning keys.
 * 
 * NOTE: It needs to call evict_key_pool_populate() for all dbs before this call.
 */
static void pick_best_key_from_key_pool(int *best_dbid, sds *best_key)
{
    serverAssert(*best_dbid == 0 && *best_key == NULL);

    struct evictKeyPoolEntry *pool = evict_key_pool;

    /* Go backward from best to worst element to evict. */
    for (int k = EVPOOL_SIZE-1; k >= 0; k--) {
        if (pool[k].key == NULL) continue;

        const int dbid = pool[k].dbid;

        dict *d = server.db[dbid].dict;
        dictEntry *de = dictFind(d, pool[k].key);

        /* Remove the entry from the pool. */
        if (pool[k].key != pool[k].cached)
            sdsfree(pool[k].key);

        pool[k].key = NULL;
        pool[k].idle = 0;

        /* Check ghost key.
         * What is the situation for ghost key?
         * The pool's element has previous value which could be changed.
         *      e.g. DEL and change anotther type 
         *           or even be evicted to RocksDB by command rockevict
         *           or even changed to rock hash
         */
        if (de) {
            const sds internal_key = dictGetKey(de);

            // NOTE: which one can be evicted is very complicated,
            //       so we call check_valid_evict_of_key_for_db()
            if (check_valid_evict_of_key_for_db(dbid, internal_key) == CHECK_EVICT_OK)
            {
                // We found the best key
                *best_dbid = dbid;
                *best_key = internal_key;
                return;
            }
        }
    }

    // We can not find the best key from the pool
}

/* Reference the above pick_best_key_from_key_pool() for similiar algorithm */
static void pick_best_field_from_hash_pool(int *best_dbid, sds *best_field, sds *best_hash_key)
{
    serverAssert(*best_dbid == 0 && *best_hash_key == NULL && *best_field == NULL);

    struct evictHashPoolEntry *pool = evict_hash_pool;

    /* Go backward from best to worst element to evict. */
    for (int k = EVPOOL_SIZE-1; k >= 0; k--) {
        if (pool[k].field == NULL) continue;

        const int dbid = pool[k].dbid;

        dict *d_db = server.db[dbid].dict;
        dictEntry *de_db = dictFind(d_db, pool[k].hash_key);
        dictEntry *de_field = NULL;
        if (de_db)
        {
            robj *o = dictGetVal(de_db);
            if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT)
                // even o is shared.rock_val_hash_ht, it is OK
                de_field = dictFind(o->ptr, pool[k].field);
        }

        /* Remove the entry from the pool. */
        if (pool[k].hash_key != pool[k].cached_hash_key)
            sdsfree(pool[k].hash_key);

        if (pool[k].field != pool[k].cached_field)
            sdsfree(pool[k].field);

        pool[k].hash_key = NULL;
        pool[k].field = NULL;
        pool[k].idle = 0;

        /* Check ghost field.
         * What is the situation for ghost field?
         * The pool's element has previous value which could be changed.
         *      e.g. DEL whole key and regenerated
         *           so even it could be a hash agian but not in rock_hash 
         *           or even be evicted to RocksDB by command rockevicthash
         */
        if (de_field) { 
            const sds internal_field = dictGetKey(de_field);
            const sds internal_hash_key = dictGetKey(de_db);

            // NOTE: which one can be evicted is very complicated,
            //       so we call check_valid_evict_of_key_for_hash()
            if (check_valid_evict_of_key_for_hash(dbid, internal_hash_key, internal_field) == CHECK_EVICT_OK)
            {
                // We found the best key
                *best_dbid = dbid;
                *best_field = internal_field;
                *best_hash_key = internal_hash_key;

                return;
            }
        }
    }

    // We can not find the best field from the pool
}

// #define MAX_WAIT_RING_BUFFER_US 10

/* Try to perform one key eviction.
 *
 * The return value is the memory size for eviction (NOTE: not actually released because the async mode)
 * 
 * if return memory size is SIZE_MAX which is not possible, it means ring buffer is full
 *    which inidcates that the write thread is busy.
 * 
 * If return memory size is zero, it means failure for such cases:
 * 1. the db->rock_evict is empty (but the caller should avoid this condition)
 * 2. can not find the best key in th pool for eviction because 2-1) and 2-2)
 *    2-1) the sample are all younger than all itmes in the previous pool
 *    2-2) the pool are all ghosts
 *    but we can retry because ghosts will leave the pool for pick_best_key_from_key_pool() call
 */
static size_t try_to_perform_one_key_eviction()
{
    size_t key_total = 0;
    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;
        key_total += dictSize(db->rock_evict);

        evict_key_pool_populate(i);
    }

    if (key_total == 0)
        return 0;

    int best_dbid = 0;
    sds best_key = NULL;
    pick_best_key_from_key_pool(&best_dbid, &best_key);
    if (best_key == NULL)
    {
        // In theory, we can not get this condition
        // but return zero is safe for this abnormal condition
        serverLog(LL_WARNING, "best_key == NULL for key eviction");
        return 0;
    }

    // We can evict the key
    size_t mem = 0;
    const int ret = try_evict_one_key_to_rocksdb(best_dbid, best_key, &mem);
    if (ret == TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL)
        return SIZE_MAX;
    
    serverAssert(ret == TRY_EVICT_ONE_SUCCESS);
    // serverLog(LL_WARNING, "evict key = %s, dbid = %d, mem = %lu", best_key, best_dbid, mem);
    
    serverAssert(mem > 0 && mem != SIZE_MAX);
    return mem;
}

/* Reference the above try_to_perform_one_key_eviction() for the similiar algorithm */
static size_t try_to_perform_one_field_eviction()
{
    size_t field_total = 0;
    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;
        field_total += db->rock_hash_field_cnt;

        evict_hash_pool_populate(i);
    }

    if (field_total == 0)
        return 0;

    int best_dbid = 0;
    sds best_field = NULL;
    sds best_hash_key = NULL;
    pick_best_field_from_hash_pool(&best_dbid, &best_field, &best_hash_key);
    if (best_field == NULL)
    {
        // In theory, we can not get this condition
        // but return zero is safe for this abnormal condition
        serverLog(LL_WARNING, "best_field == NULL for field eviction");
        return 0;
    }

    // We can evict the field
    size_t mem = 0;
    const int ret = try_evict_one_field_to_rocksdb(best_dbid, best_hash_key, best_field, &mem);
    if (ret == TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL)
        return SIZE_MAX;

    serverAssert(ret == TRY_EVICT_ONE_SUCCESS);

    // serverLog(LL_WARNING, "evict field = %s, best_hash_key = %s, dbid = %d, mem = %lu", 
    //          best_field, best_hash_key, best_dbid, mem);
    
    serverAssert(mem > 0 && mem != SIZE_MAX);
    return mem;
}

#ifdef RED_ROCK_EVICT_INFO

static int ei_try_cnt = 0;
static int ei_noavail_cnt = 0;
static int ei_rbuf_timeout_cnt = 0;
static int ei_free_timeout_cnt = 0;
static int ei_success_cnt = 0;
static size_t ei_success_total_us = 0;
static size_t ei_timeout_cnt = 0;


void eviction_info_print()
{
    serverLog(LL_WARNING, "try_cnt = %d, noavail_cnt = %d, rbuf_timeout_cnt = %d, "
                          "success_cnt = %d, success_total_us = %zu, avg success (us) = %zu, "
                          "free_timeout_cnt = %d, avg key/field cnt of free_time_out = %zu, timeout_cnt(key/field) = %zu",
                          ei_try_cnt, ei_noavail_cnt, ei_rbuf_timeout_cnt,  
                          ei_success_cnt, ei_success_total_us, ei_success_cnt == 0 ? 0 : ei_success_total_us/(size_t)ei_success_cnt,
                          ei_free_timeout_cnt, ei_free_timeout_cnt == 0 ? 0 : ei_timeout_cnt/(size_t)ei_free_timeout_cnt, ei_timeout_cnt);
}

#endif

/* Try to evict keys to the want_to_free size
 *
 * 1. if we calculate the evict size (because aysnc mode and approximate esitmate) 
 *    over want_to_free, returnn
 * 2. or if timeout (10ms), return. Timeout could be for the busy of RocksDB write
 */
static size_t perform_key_eviction(const size_t want_to_free, const unsigned int timeout_us)
{
    #ifdef RED_ROCK_EVICT_INFO
    ++ei_try_cnt;
    int cnt_for_this_eviction = 0;
    int timeout_of_eviction = 0;
    #endif

    size_t free_total = 0;
    monotime timer;
    elapsedStart(&timer);
    
    while (free_total < want_to_free)
    {
        size_t will_free_mem = try_to_perform_one_key_eviction();
        if (will_free_mem == 0)
        {
            #ifdef RED_ROCK_EVICT_INFO
            ++ei_noavail_cnt;
            #endif
            // In theory, it means no key avaiable for eviction
            // while the memory is not enough
            serverLog(LL_WARNING, "No available keys for eviction!");
            break;
        }

        if (will_free_mem == SIZE_MAX)
        {
            #ifdef RED_ROCK_EVICT_INFO
            ++ei_rbuf_timeout_cnt;
            #endif
            break;      // timeout for waitng for ring buffer
        }

        #ifdef RED_ROCK_EVICT_INFO
        ++cnt_for_this_eviction;
        #endif

        free_total += will_free_mem;

        if (free_total < want_to_free)
        {
            const uint64_t elapse_us = elapsedUs(timer);
            if (elapse_us > timeout_us)
            {           
                // timeouut
                #ifdef RED_ROCK_EVICT_INFO     
                timeout_of_eviction = 1;
                serverLog(LL_WARNING, "perform_key_eviction() timeout for %zu (us), but alreay evict mem = %lu, want_to_free = %zu", 
                                      elapse_us, free_total, want_to_free);  
                #endif 
                break;     
            }
        }
    }

#ifdef RED_ROCK_EVICT_INFO
    if (free_total >= want_to_free)
    {
        ++ei_success_cnt;
        ei_success_cnt += cnt_for_this_eviction;
        ei_success_total_us += elapsedUs(timer);
    }
    else
    {
        if (timeout_of_eviction)
        {
            ++ei_free_timeout_cnt;;
            ei_timeout_cnt += cnt_for_this_eviction;
        }
    }
#endif
    
    return free_total;
}

/* Reference the above perform_key_eviction() for similiar algorithm */
static size_t perform_field_eviction(const size_t want_to_free, const unsigned int timeout_us)
{
    #ifdef RED_ROCK_EVICT_INFO
    ++ei_try_cnt;
    int cnt_for_this_eviction = 0;
    int timeout_of_eviction = 0;
    #endif

    size_t free_total = 0;
    monotime timer;
    elapsedStart(&timer);

    while (free_total < want_to_free)
    {
        size_t will_free_mem = try_to_perform_one_field_eviction();
        if (will_free_mem == 0)
        {
            #ifdef RED_ROCK_EVICT_INFO
            ++ei_noavail_cnt;
            #endif
            // In theory, it means no field avaiable for eviction
            // while the memory is not enough
            serverLog(LL_WARNING, "No available fields for eviction!");
            break;
        }

        if (will_free_mem == SIZE_MAX)
        {
            #ifdef RED_ROCK_EVICT_INFO
            ++ei_rbuf_timeout_cnt;
            #endif
            break;  // timeout for waitng for ring buffer
        }

        #ifdef RED_ROCK_EVICT_INFO
        ++cnt_for_this_eviction;
        #endif

        free_total += will_free_mem;

        if (free_total < want_to_free)
        {
            const uint64_t elapse_us = elapsedUs(timer);
            if (elapse_us > timeout_us)
            {
                // timeout
                #ifdef RED_ROCK_EVICT_INFO     
                timeout_of_eviction = 1;                
                serverLog(LL_WARNING, "perform_field_eviction() timeout for %zu (us), but alreay evict mem = %zu, want_to_free = %zu", 
                                      elapse_us, free_total, want_to_free); 
                #endif  
                break;     
            }
        }
    }

#ifdef RED_ROCK_EVICT_INFO
    if (free_total >= want_to_free)
    {
        ++ei_success_cnt;
        ei_success_cnt += cnt_for_this_eviction;
        ei_success_total_us += elapsedUs(timer);
    }
    else
    {
        if (timeout_of_eviction)
        {
            ++ei_free_timeout_cnt;;
            ei_timeout_cnt += cnt_for_this_eviction;
        }
    }
#endif

    return free_total;
}

/* return 1 to choose key eviction, 0 to choose field eviction */
static int choose_key_or_field_eviction()
{
    size_t key_cnt = 0;
    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;
        key_cnt += dictSize(db->rock_evict);
    }

    size_t field_cnt = 0;
    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;
        field_cnt += db->rock_hash_field_cnt;
    }

    return key_cnt >= field_cnt;
}

#define EVICTION_MIN_TIMEOUT_US (1<<10)             // about 1 ms
#define EVICTION_MAX_TIMEOUT_US (1<<12)             // about 4 ms
void perform_rock_eviction()
{
    #ifdef RED_ROCK_EVICT_INFO
    static monotime timer;
    static int timing_in_process = 0;    
    #endif

    static unsigned int timeout = EVICTION_MIN_TIMEOUT_US;
    static long long last_stat_numcommands = 0L;
    
    if (server.maxrockmem == 0)
        return;

    size_t used = zmalloc_used_memory();
    if (used <= server.maxrockmem)
    {
        #ifdef RED_ROCK_EVICT_INFO
        if (timing_in_process)
            serverLog(LL_WARNING, "eviction debug info: total seconds = %zu", elapsedMs(timer)/1000);

        timing_in_process = 0;
        #endif        
        
        return;
    }

    #ifdef RED_ROCK_EVICT_INFO
    if (!timing_in_process)
    {
        elapsedStart(&timer);
        timing_in_process = 1;   
    } 
    #endif

    const size_t want_to_free = used - server.maxrockmem;

    if (server.stat_numcommands != last_stat_numcommands)
    {
        // If there are some commands in the periood, i.e., server is busy, 
        // tiemout needs to be go back to 1 ms
        last_stat_numcommands = server.stat_numcommands;
        timeout = EVICTION_MIN_TIMEOUT_US;
    }
    else
    {
        // othewise, server is idle, we can double the timeout until 8 ms
        timeout <<= 1;
        if (timeout > EVICTION_MAX_TIMEOUT_US)
            timeout = EVICTION_MAX_TIMEOUT_US;
    }

    if (choose_key_or_field_eviction())
    {
        perform_key_eviction(want_to_free, timeout);
    }
    else
    {
        perform_field_eviction(want_to_free, timeout);
    }
}