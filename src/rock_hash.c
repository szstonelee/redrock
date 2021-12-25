#include "rock_hash.h"
#include "rock.h"


/* For rockHashDictType, each db has just one instance.
 * When a hash with OBJ_ENCODING_HT reach a threeshold, i.e., server.hash_max_rock_entries,
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
 * key is the fiied sds, shared with the corresponding hash's field.
 * value is the recent visited clock time saved as a format of pointer.
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


static void debug_check_lru(dict *hash, dict *lrus)
{
    size_t sz_hash = dictSize(hash);
    size_t sz_lrus = dictSize(lrus);

    size_t sz_rock = 0;

    dictIterator *di_hash = dictGetIterator(hash);
    dictEntry *de_hash;
    while ((de_hash = dictNext(di_hash)))
    {
        sds field = dictGetKey(de_hash);
        sds val = dictGetVal(de_hash);
        if (val != shared.hash_rock_val_for_field)
        {
            serverAssert(dictFind(lrus, field));
        }
        else
        {
            serverAssert(!dictFind(lrus, field));
            ++sz_rock;
        }
    }
    dictReleaseIterator(di_hash);
    serverAssert(sz_rock + sz_lrus == sz_hash);
}

/* Add a whole hash to rock hash by allocating the lrus for all fields.
 * The caller guarantee that the hash not exist in rock_hash
 * and the internal_redis_key is the db->dict key for the hash.
 */
static void add_whole_hash_to_rock_hash(const sds internal_redis_key, const dict *hash, redisDb *db)
{
    #if defined RED_ROCK_DEBUG
    dictEntry *de_check = dictFind(db->dict, internal_redis_key);
    serverAssert(de_check);
    serverAssert(dictGetKey(de_check) == internal_redis_key);
    #endif

    const uint64_t clock = LRU_CLOCK();

    dict *lrus = dictCreate(&fieldLruDictType, NULL);
    dictIterator *di_hash = dictGetIterator((dict*)hash);
    dictEntry *de_hash;
    while ((de_hash = dictNext(di_hash)))
    {
        const sds internal_field = dictGetKey(de_hash);
        serverAssert(dictGetVal(de_hash) != shared.hash_rock_val_for_field);
        serverAssert(dictAdd(lrus, internal_field, (void*)clock) == DICT_OK);
    }
    dictReleaseIterator(di_hash);

    serverAssert(dictAdd(db->rock_hash, internal_redis_key, lrus) == DICT_OK);    
}

/* After a hash key add a field, it will call here to determine
 * whether it needs to add itself to db->rock_hash.
 * If the server disable the feature, do nothing.
 * If the hash object (i.e., o) is not match the threshhold condition, do nothing.
 * If the key already in db->rock_hash, add lru (key is the field)
 * Otherwise, create the dict of valid lrus for all fields.
 * 
 * NOTE:
 *      We will use the internal key in redis DB and field in hahs
 *      because rock_hash shared them.
 */
void on_hash_key_add_field(const int dbid, const sds redis_key, const robj *o, const sds field)
{
    serverAssert(o->type == OBJ_HASH);


    if (o->encoding != OBJ_ENCODING_HT)
        return;

    if (server.hash_max_rock_entries == 0)
        return;

    dict *hash = o->ptr;
    if (dictSize(hash) <= server.hash_max_rock_entries)
        return;

    redisDb *db = server.db + dbid;

    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);
    const sds internal_redis_key = dictGetKey(de_db);
    serverAssert(dictGetVal(de_db) == o);

    dictEntry *de_rock_hash = dictFind(db->rock_hash, internal_redis_key);
    if (de_rock_hash)
    {
        const uint64_t clock = LRU_CLOCK();
        dict *lrus = dictGetVal(de_rock_hash);
        dictEntry *de_hash = dictFind(hash, field);
        serverAssert(de_hash);
        const sds internal_field = dictGetKey(de_hash);
        serverAssert(dictAdd(lrus, internal_field, (void*)clock) == DICT_OK);
        #if defined RED_ROCK_DEBUG
        debug_check_lru(o->ptr, lrus);
        #endif
    }
    else
    {
        // create a dict of lrus, add all fields to the lrus
        // then add redis_key and lrus to rock_hash 
        add_whole_hash_to_rock_hash(internal_redis_key, hash, db);
    }
}

/* When a hash already delete a field.
 * Because server.hash_max_rock_entries can change in runtime, 
 * we must check rock hash.
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
        #if defined RED_ROCK_DEBUG
        debug_check_lru(o->ptr, lrus);
        #endif
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
        #if defined RED_ROCK_DEBUG
        debug_check_lru(o->ptr, lrus);
        #endif
    }
}

/* When rock_read.c already recover a field from RocksDB, it needs to add itself to rock hash
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
    #if defined RED_ROCK_DEBUG
    debug_check_lru(o->ptr, lrus);
    #endif
}

/* When rock_write.c already set one field's value to rock value.
 */
void on_rockval_field_of_hash(const int dbid, const sds redis_key, const robj *o, const sds field)
{
    serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);

    redisDb *db = server.db + dbid;
    dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);
    serverAssert(de_rock_hash);
    dict *lrus = dictGetVal(de_rock_hash);
    serverAssert(dictDelete(lrus, field) == DICT_OK);
    #if defined RED_ROCK_DEBUG
    debug_check_lru(o->ptr, lrus);
    #endif    
}

/* Before or after a redis db delete a hash with the key of redis_key
 * NOTE: It could be del a key or overwrite a key for the redis db.
 */
void on_del_hash_from_db(const int dbid, const sds redis_key)
{
    redisDb *db = server.db + dbid;
    dictDelete(db->rock_hash, redis_key);
}

/* If in rock hash, return 1.
 * Otherwise, return 0.
 */
int is_in_rock_hash(const int dbid, const sds redis_key)
{
    redisDb *db = server.db + dbid;
    return dictFind(db->rock_hash, redis_key) != NULL;
}

/* When redis server start and load RDB/AOF,
 * we need to add the matched hash to rock hash.
 */
void init_rock_hash_before_enter_event_loop()
{
    if (server.hash_max_rock_entries == 0)
        return;

    const size_t threshold = server.hash_max_rock_entries;

    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;
        dict* key_space = db->dict;

        dictIterator *di = dictGetIterator(key_space);
        dictEntry *de;
        while ((de = dictNext(di)))
        {
            robj *o = dictGetVal(de);
            if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT)
            {
                dict *hash = o->ptr;
                if (dictSize(hash) > threshold)
                {
                    sds internal_redis_key = dictGetKey(de);
                    add_whole_hash_to_rock_hash(internal_redis_key, hash, db);
                }
            }
        }
        dictReleaseIterator(di);
    }
}