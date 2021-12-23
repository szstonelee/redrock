#include "rock_hash.h"
#include "rock.h"


/* key is db redis key, (type is OBJ_HASH and encoding OBJ_ENCODING_HT), 
 *     and has enough fields. Shared with db->dict, so do not need key destructor.
 *     If db redis key match the condition, it will enter this dict and 
 *     never quit (even the field number drop to the threshold) 
 *     until the key is deleted from redis db.
 * value is pointer to a dict with valid lru (not evict to RocksDB). 
 */
int dictExpandAllowed(size_t moreMem, double usedRatio);    // declaration in server.c
static void val_as_dict_destructor(void *privdata, void *obj)
{
    UNUSED(privdata);
    dictRelease((dict*)obj);
}
dictType rockHashDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    val_as_dict_destructor,     /* val destructor */
    dictExpandAllowed           /* allow to expand */
};

/* API for server initianization of db->rock_hash for each redisDB, 
 * like db->dict, db->expires */
dict* init_rock_hash_dict()
{
    return dictCreate(&rockHashDictType, NULL);
}

/* For dict with valid lru,
 * key is the fiied sds, shared with the corresponding hash.
 * value is the lru value for the field.  
 */
dictType fieldLruDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL,                       /* val destructor */
    dictExpandAllowed           /* allow to expand */
};

static void debug_check_sds_equal(const int dbid, const sds redis_key, const robj *o, const sds field)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);
    serverAssert(dictGetKey(de_db) == redis_key);
    serverAssert(dictGetVal(de_db) == o);

    dict *hash = o->ptr;
    dictEntry *de_hash = dictFind(hash, field);
    serverAssert(de_hash);
    serverAssert(dictGetKey(de_hash) == field);
}

/* After a hash key add a field, it will call here to determine
 * whether it needs to add itself to db->rock_hash.
 * If the server disable the feature, do nothing.
 * If the key already in db->rock_hash, add lru (key is the field)
 * If the hash object (i.e., o) is not match the condition, do nothing.
 * Otherwise, create the dict of valid lru for all fields.
 * 
 * NOTE:
 * 1. redis_key must be the key of sds in redis db dict because sharing.
 * 2. field must be the field of sds in internal dict of o becausse sharing.
 */
void on_hash_key_add_field(const int dbid, const sds redis_key, const robj *o, const sds field)
{
    serverAssert(o->type == OBJ_HASH);

    if (o->encoding != OBJ_ENCODING_HT)
        return;

    #if defined RED_ROCK_DEBUG
    debug_check_sds_equal(dbid, redis_key, o, field);
    #endif

    if (server.hash_max_rock_entries == 0)
        return;

    dict *hash = o->ptr;
    if (dictSize(hash) <= server.hash_max_rock_entries)
        return;

    redisDb *db = server.db + dbid;
    uint64_t clock = LRU_CLOCK();
    dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);
    if (de_rock_hash)
    {
        dict *lrus = dictGetVal(de_rock_hash);
        serverAssert(dictAdd(lrus, field, (void*)clock) == DICT_OK);
    }
    else
    {
        // create a dict of lrus, add all fields to the lrus
        // then add redis_key and lrus to rock_hash 
        dict *hash = o->ptr;
        dict *lrus = dictCreate(&fieldLruDictType, NULL);
        dictIterator *di_hash = dictGetIterator(hash);
        dictEntry* de_hash;
        while ((de_hash = dictNext(di_hash)))
        {
            const sds field = dictGetKey(de_hash);
            serverAssert(dictAdd(lrus, field, (void*)clock) == DICT_OK);
        }
        dictReleaseIterator(di_hash);
        serverAssert(dictAdd(db->rock_hash, redis_key, lrus) == DICT_OK);    
    }
}

/* When a hash delete a field.
 * Because server.hash_max_rock_entries can change, we must check rock hash.
 */
void on_hash_key_del_field(const int dbid, const sds redis_key, const robj *o, const sds field)
{
    serverAssert(o->type == OBJ_HASH);

    if (o->encoding != OBJ_ENCODING_HT)
        return;

    redisDb *db = server.db + dbid;
    dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);
    if (de_rock_hash)
    {
        dict *lrus = dictGetVal(de_rock_hash);
        dictDelete(lrus, field);
    }
}

/* When a client visit a field of a hash. So we need to update the lru clock.
 */
void on_visit_field_of_hash(const int dbid, const sds redis_key, const robj *o, const sds field)
{
    serverAssert(o->type == OBJ_HASH);

    if (o->encoding != OBJ_ENCODING_HT)
        return;

    redisDb *db = server.db + dbid;
    dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);
    if (de_rock_hash)
    {
        dict *lrus = dictGetVal(de_rock_hash);
        dictEntry *de_lru = dictFind(lrus, field);
        if (de_lru)
        {
            uint64_t clock = LRU_CLOCK();
            dictGetVal(de_lru) = (void*)clock;
        }
    }
}

/* When rock_read.c recover a field from RocksDB, it needs to add itself to rock hash
 */
void on_recover_field_of_hash(const int dbid, const sds redis_key, const robj *o, const sds field)
{
    serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);

    redisDb *db = server.db + dbid;
    dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);
    serverAssert(de_rock_hash);
    dict *lrus = dictGetVal(de_rock_hash);
    uint64_t clock = LRU_CLOCK();
    serverAssert(dictAdd(lrus, field, (void*)clock) == DICT_OK);
}

/* When a db delete a hash with the key of redis_key
 */
void on_del_hash_from_db(const int dbid, const sds redis_key)
{
    redisDb *db = server.db + dbid;
    dictDelete(db->rock_hash, redis_key);
}

