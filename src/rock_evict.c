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

void on_db_add_key_for_rock_evict(const int dbid, const sds key)
{
    UNUSED(dbid);
    UNUSED(key);
}

void on_db_del_key_for_rock_evict(const int dbid, const sds key)
{
    UNUSED(dbid);
    UNUSED(key);
}

void on_db_overwrite_key_for_rock_evict(const int dbid, const sds key)
{
    UNUSED(dbid);
    UNUSED(key);
}

void on_db_visit_key_for_rock_evict(const int dbid, const sds key)
{
    UNUSED(dbid);
    UNUSED(key);
}

void on_rockval_key_for_rock_evict(const int dbid, const sds key)
{
    UNUSED(dbid);
    UNUSED(key);
}

void on_recover_key_for_rock_evict(const int dbid, const sds key)
{
    UNUSED(dbid);
    UNUSED(key);
}

void on_empty_db_for_rock_evict(const int dbnum)
{
    UNUSED(dbnum);
}
