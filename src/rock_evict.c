#include "rock_evict.h"
#include "rock.h"
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
    if (de == NULL)
        return;     // not found in redis db

#if defined RED_ROCK_DEBUG
    robj *o = dictGetVal(de);

    if (is_rock_value(o))
    {
        serverAssert(dictFind(db->rock_evict, key) == NULL);
        return;
    }

    if (!is_rock_type(o))
    {
        serverAssert(dictFind(db->rock_evict, key) == NULL);
        return;
    }

    if (is_shared_value(o))
    {
        serverAssert(dictFind(db->rock_evict, key) == NULL);
        return;
    }

    if (is_in_rock_hash(dbid, key))
    {
        serverAssert(dictFind(db->rock_evict, key) == NULL);
        return;
    }
#endif

    int ret  = dictDelete(db->rock_evict, key);

#if defined RED_ROCK_DEBUG
    serverAssert(ret == DICT_OK);
#endif
}

/* After a key is overwritten in redis db, it will call here.
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

    // NOTE: because on_overwrite_key_from_db_for_rock_hash()
    //       call before on_db_overwrite_key_for_rock_evict()
    if (is_in_rock_hash(dbid, key))
        return;

    sds internal_key = dictGetKey(de);
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
#define SAMPLE_NUMBER           5       // like Redis sample number

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
    sds cached_field;     /* Cached SDS object for field. */
    sds key;
    sds cached_key;       /* Cached SDS object for key. */
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
        ep[i].key = NULL;
        ep[i].field = NULL;
        ep[i].cached_field = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[i].cached_key = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
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
static size_t evict_key_pool_populate(const int dbid)
{
    // first, sample some keys from rock_evict
    redisDb *db = server.db + dbid;
    if (dictSize(db->rock_evict) == 0)
        return 0;

    dictEntry* sample_des[SAMPLE_NUMBER];
    const unsigned int count = dictGetSomeKeys(db->rock_evict, sample_des, SAMPLE_NUMBER);
    serverAssert(count != 0);

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
                memmove(pool+k+1,pool+k,
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
                memmove(pool,pool+1,sizeof(pool[0])*k);
                pool[k].cached = cached;
            }
            ++insert_cnt;
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        int klen = sdslen(key);
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

/* From the key pool, find the right key to be evicted.
 * Because the pool has previous values, so we need to take care of it.
 * And we also clear the pool for scanning keys.
 * 
 * NOTE: It needs to call evict_key_pool_populate() before this call.
 */
static void pick_best_key_from_key_pool(int *best_dbid, sds *best_key)
{
    serverAssert(*best_dbid == 0 && *best_key == NULL);

    struct evictKeyPoolEntry *pool = evict_key_pool;

    /* Go backward from best to worst element to evict. */
    for (int k = EVPOOL_SIZE-1; k >= 0; k--) {
        if (pool[k].key == NULL) continue;

        const int dbid = pool[k].dbid;

        dict *d = server.db[pool[k].dbid].dict;
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
            robj *o = dictGetVal(de);
            sds internal_key = dictGetKey(de);

            // NOTE: is_shared_value() guarantee !is_rock_value()
            if (!is_shared_value(o) &&
                is_rock_type(o) &&
                !is_in_rock_hash(dbid, internal_key))
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

/* Try to perform one key eviction.
 *
 * The return value is the memory size for eviction (NOTE: not actually released because the async mode)
 * 
 * If memory size is zero, it means failure for such cases:
 * 1. the db->rock_evict is empty (but the caller should avoid this condition)
 * 2. can not find the best key in th pool for eviction because 2-1) and 2-2)
 *    2-1) the sample are all younger than all itmes in the previous pool
 *    2-2) the pool are all ghosts
 *    but we can retry because ghosts will leave the pool for pick_best_key_from_key_pool() call
 */
static size_t try_to_perform_one_key_eviction()
{
    static int key_loop_dbid = 0;   // loop for each db

    const int dbnum = server.dbnum;
    int all_db_not_available = 1;
    for (int i = 0; i < dbnum; ++i)
    {
        const int ret = evict_key_pool_populate(key_loop_dbid);
        if (ret != 0)
        {
            all_db_not_available = 0;
            break;
        }
        
        ++key_loop_dbid;
        if (key_loop_dbid == dbnum)
            key_loop_dbid = 0;
    }

    if (all_db_not_available)
        return 0;

    int best_dbid = 0;
    sds best_key = NULL;
    pick_best_key_from_key_pool(&best_dbid, &best_key);
    if (best_key == NULL)
        return 0;

    // We can evict the key
    size_t mem = 0;
    int not_write_success = 1;
    while (not_write_success)
    {
        const int ret = try_evict_one_key_to_rocksdb(best_dbid, best_key, &mem);
        switch (ret)
        {
        case TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL:
            break;      // try again until success
        case TRY_EVICT_ONE_SUCCESS:
            not_write_success = 0;
            break;
        default:
            serverPanic("perform_one_key_eviction()");
        }
    }
    
    return mem;
}

/* Try to evict keys to the want_to_free size
 *
 * 1. if we calculate the evict size (because aysnc mode and approximate esitmate) 
 *    over want_to_free, returnn
 * 2. or if timeout (10ms), return. Timeout could be for the busy of RocksDB write
 */
static void perform_key_eviction(const size_t want_to_free)
{
    int finish = 0;
    size_t free_total = 0;
    monotime evictionTimer;
    elapsedStart(&evictionTimer);
    int try_fail_cnt = 0;

    while (!finish)
    {
        size_t will_free_mem = try_to_perform_one_key_eviction();
        if (will_free_mem == 0)
        {
            ++try_fail_cnt;
            if (try_fail_cnt >= 2)
                serverLog(LL_WARNING, "try_to_perform_one_key_eviction() return 2 failure continously!");
        }
        else
        {
            try_fail_cnt = 0;
        }

        free_total += will_free_mem;

        if (free_total >= want_to_free)
        {
            finish = 1;
        }
        else
        {
            if (elapsedMs(evictionTimer) >= 10)
                // timeout
                finish = 1;    
                serverLog(LL_WARNING, "perform_key_eviction() timeout");        
        }
    }
}