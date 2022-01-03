#include "rock_evict.h"
#include "rock.h"
#include "rock_hash.h"

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

#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255

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

/* Create a new eviction pool for string or ziplist */
static void evict_key_pool_alloc(void) 
{
    struct evictKeyPoolEntry *ep;

    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (int i = 0; i < EVPOOL_SIZE; ++i) 
    {
        ep[i].idle = 0;
        ep[i].key = NULL;
        ep[i].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[i].dbid = 0;
    }
    evict_key_pool = ep;
}

/* Create a new eviction pool for pure hash. */
static void evict_hash_pool_alloc(void) {
    struct evictHashPoolEntry *ep;

    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
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
