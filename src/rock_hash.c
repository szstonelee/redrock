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
void val_as_dict_destructor(void *privdata, void *obj)
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

static void debug_print_lrus(const dict *lrus)
{
    dictIterator *di = dictGetIterator((dict*)lrus);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        const sds field = dictGetKey(de);
        serverLog(LL_NOTICE, "debug_print_lrus, field = %s", field);
    }

    dictReleaseIterator(di);
}

static void debug_check_lru(const char *from, dict *hash, dict *lrus, const sds will_delete_field)
{
    size_t sz_hash = dictSize(hash);
    size_t sz_lrus = dictSize(lrus);

    size_t sz_rock = 0;

    dictIterator *di_hash = dictGetIterator(hash);
    dictEntry *de_hash;
    while ((de_hash = dictNext(di_hash)))
    {
        const sds field = dictGetKey(de_hash);
        if (will_delete_field)
        {
            if (sdscmp(field, will_delete_field) == 0)
                continue;       // will ignore the will_delete_field for the hashs
        }

        const sds val = dictGetVal(de_hash);
        if (val != shared.hash_rock_val_for_field)
        {
            if (dictFind(lrus, field) == NULL)
            {
                debug_print_lrus(lrus);
                serverPanic("debug_check_lru, from = %s, val != shared.hash_rock_val_for_field, field = %s", from, field);
            }
        }
        else
        {
            if (dictFind(lrus, field) != NULL)
            {
                debug_print_lrus(lrus);
                serverPanic("debug_check_lru, from = %s, val == shared.hash_rock_val_for_field, field = %s, will_delete_field = %s", 
                            from, field, will_delete_field);
            }

            ++sz_rock;
        }
    }
    dictReleaseIterator(di_hash);

    if (will_delete_field)
    {
        serverAssert(sz_rock + sz_lrus + 1 == sz_hash);
    }
    else
    {
        serverAssert(sz_rock + sz_lrus == sz_hash);
    }
    
}

/* Add a hash in redis db to rock hash by allocating the lrus for all fields in the hash.
 *
 * The caller guarantee that: 
 * 1. The reids_key is a hash with correct format 
 * 2. the redis_key not exist in rock_hash
 * 
 * NOTE: for rock_hash, we need to use internal_redis_key and internal_fields 
 */
static void add_whole_redis_hash_to_rock_hash(const int dbid, const sds redis_key)
{
    serverAssert(server.hash_max_rock_entries > 0);

    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);

    const sds internal_redis_key = dictGetKey(de_db);
    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);
    dict *hash = o->ptr;

    dict *lrus = dictCreate(&fieldLruDictType, NULL);

    const uint64_t clock = LRU_CLOCK();
    dictIterator *di_hash = dictGetIterator((dict*)hash);
    dictEntry *de_hash;
    while ((de_hash = dictNext(di_hash)))
    {
        const sds internal_field = dictGetKey(de_hash);
        serverAssert(dictGetVal(de_hash) != shared.hash_rock_val_for_field);
        serverAssert(dictAdd(lrus, internal_field, (void*)clock) == DICT_OK);
    }
    dictReleaseIterator(di_hash);

    serverAssert(dictSize(lrus) > server.hash_max_rock_entries);
    serverAssert(dictAdd(db->rock_hash, internal_redis_key, lrus) == DICT_OK);    
}

/* After a hash key add a field, it will call here to determine
 * whether it needs to add the field to db->rock_hash.
 * If the server disable the feature, do nothing.
 * If the hash object (i.e., o) is not match the threshhold condition, do nothing.
 * If the key already in db->rock_hash, add lru (key is the field)
 * Otherwise, create the dict of valid lrus for all fields.
 * 
 * NOTE:
 *      We will use the internal key in redis DB and 
 *      internal field in the hash because rock_hash shared them.
 */
void on_hash_key_add_field(const int dbid, const sds redis_key, const sds field)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);

    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH);

    if (o->encoding != OBJ_ENCODING_HT)
        return;

    dict *hash = o->ptr;

    const sds internal_redis_key = dictGetKey(de_db);

    dictEntry *de_rock_hash = dictFind(db->rock_hash, internal_redis_key);
    if (de_rock_hash)
    {
        dictEntry *de_hash = dictFind(hash, field);
        serverAssert(de_hash);
        const sds internal_field = dictGetKey(de_hash);

        const uint64_t clock = LRU_CLOCK();
        dict *lrus = dictGetVal(de_rock_hash);

        serverAssert(dictAdd(lrus, internal_field, (void*)clock) == DICT_OK);

        #if defined RED_ROCK_DEBUG
        debug_check_lru("on_hash_key_add_field", o->ptr, lrus, NULL);
        #endif
    }
    else
    {
        if (server.hash_max_rock_entries == 0 || dictSize(hash) <= server.hash_max_rock_entries)
            return;    
        // create a dict of lrus, add all fields to the lrus
        // then add redis_key and lrus to rock_hash 
        add_whole_redis_hash_to_rock_hash(dbid, internal_redis_key);
    }
}

/* Before a hash delete a field (because the field may be freed!!!!)
 * Because server.hash_max_rock_entries can change in runtime 
 * (like set to 0 when the redis_key already in the rock_hash), 
 * we must check rock hash and ignore the server.hash_max_rock_entries.
 */
void on_hash_key_del_field(const int dbid, const sds redis_key, const sds field)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);

    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH);

    if (o->encoding != OBJ_ENCODING_HT)
    {
        #if defined RED_ROCK_DEBUG
        if (dictFind(db->rock_hash, redis_key))
            serverPanic("on_hash_key_del_field, %s encoding wrong!", redis_key);
        #endif
        return;
    }

    dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);
    if (de_rock_hash)
    {
        dict *lrus = dictGetVal(de_rock_hash);
        dictDelete(lrus, field);

        #if defined RED_ROCK_DEBUG
        debug_check_lru("on_hash_key_del_field", o->ptr, lrus, field);
        #endif
    }
}

/* When a client visit a field of a hash. So we need to update the lru clock.
 */
void on_visit_field_of_hash(const int dbid, const sds redis_key, const sds field)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);

    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH);

    if (o->encoding != OBJ_ENCODING_HT)
    {
        #if defined RED_ROCK_DEBUG
        if (dictFind(db->rock_hash, redis_key))
            serverPanic("on_hash_key_del_field, %s encoding wrong!", redis_key);
        #endif
        return;
    }

    dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);
    if (de_rock_hash)
    {
        dict *lrus = dictGetVal(de_rock_hash);
        dictEntry *de_lru = dictFind(lrus, field);
        if (de_lru)
        {
            #if defined RED_ROCK_DEBUG
            dict *hash = o->ptr;
            if (dictFind(hash, field) == NULL)
                serverPanic("on_visit_field_of_hash() field in lrus but not in hash, field = %s", field);
            #endif
            uint64_t clock = LRU_CLOCK();
            dictGetVal(de_lru) = (void*)clock;
        }
        #if defined RED_ROCK_DEBUG
        debug_check_lru("on_visit_field_of_hash", o->ptr, lrus, NULL);
        #endif
    }
}

/* After a field is overwritten, it will call here.
 *
 * If is_field_rock_value_before is false(0), it means a simple overwrite.
 * We need to whether check the redis_key is in rock hash and update the lru clock.
 * 
 * If is_field_rock_value_before is true, we need to add the field to lrus of the rock hash. 
 */
void on_overwrite_field_for_rock_hash(const int dbid, const sds redis_key, const sds field, const int is_field_rock_value_before)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);

    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH);

    if (o->encoding != OBJ_ENCODING_HT)
    {
        #if defined RED_ROCK_DEBUG
        if (dictFind(db->rock_hash, redis_key))
            serverPanic("on_overwrite_field_for_rock_hash, %s encoding wrong!", redis_key);
        #endif
        return;
    }

    uint64_t clock = LRU_CLOCK();
    if (is_field_rock_value_before)
    {
        // it must have a redis_key in rock hash
        dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);
        serverAssert(de_rock_hash);
        dict *lrus = dictGetVal(de_rock_hash);
        serverAssert(dictAdd(lrus, field, (void*)clock) == DICT_OK);
    }
    else
    {
        dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);
        if (de_rock_hash)
        {
            dict *lrus = dictGetVal(de_rock_hash);
            dictEntry *de_lru = dictFind(lrus, field);
            serverAssert(de_lru);
            dictGetVal(de_lru) = (void*)clock;
        }
    }
}

/* When rock_read.c already recover a field from RocksDB, it needs to add itself to rock hash
 *
 * NOTE: When add to lrus, we need the interal field from db's hash becuase they share
 */
void on_recover_field_of_hash(const int dbid, const sds redis_key, const sds field)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);

    const sds internal_redis_key = dictGetKey(de_db);
    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);

    dict *hash = o->ptr;
    dictEntry *de_hash = dictFind(hash, field);
    serverAssert(de_hash);
    const sds internal_field = dictGetKey(de_hash);

    dictEntry *de_rock_hash = dictFind(db->rock_hash, internal_redis_key);
    serverAssert(de_rock_hash);
    dict *lrus = dictGetVal(de_rock_hash);

    uint64_t clock = LRU_CLOCK();
    // internal_field must not exist in lrus becuase of on_rockval_field_of_hash()
    serverAssert(dictAdd(lrus, internal_field, (void*)clock) == DICT_OK);   

    #if defined RED_ROCK_DEBUG
    debug_check_lru("on_recover_field_of_hash", o->ptr, lrus, NULL);
    #endif
}

/* When rock_write.c already set one field's value to rock value.
 */
void on_rockval_field_of_hash(const int dbid, const sds redis_key, const sds field)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);

    const sds internal_redis_key = dictGetKey(de_db);
    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);

    #if defined RED_ROCK_DEBUG
    dict *hash = o->ptr;
    dictEntry *de_hash = dictFind(hash, field);
    if (de_hash == NULL)
        serverPanic("on_rockval_field_of_hash() the field is not in hash, redis_key = %s, ield = %s", redis_key, field);
    serverAssert(dictGetVal(de_hash) == shared.hash_rock_val_for_field);
    #endif

    dictEntry *de_rock_hash = dictFind(db->rock_hash, internal_redis_key);
    serverAssert(de_rock_hash);
    dict *lrus = dictGetVal(de_rock_hash);
    if (dictDelete(lrus, field) != DICT_OK)
    {
        debug_print_lrus(lrus);
        serverPanic("on_rockval_field_of_hash(), key = %s, field = %s", redis_key, field);
    }

    #if defined RED_ROCK_DEBUG
    debug_check_lru("on_rockval_field_of_hash", o->ptr, lrus, NULL);
    #endif    
}

static void debug_check_rock_hash(const int dbid, const sds redis_key)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_rock_hash = dictFind(db->rock_hash, redis_key);

    if (de_rock_hash)
    {
        sds hash_key = dictGetKey(de_rock_hash);
        dictEntry *de_db = dictFind(db->dict, redis_key);
        serverAssert(de_db);
        serverAssert(dictGetKey(de_db) == hash_key);    // must be internal key
        robj *o = dictGetVal(de_db);
        serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);
        dict *hash = o->ptr;

        dict *lrus = dictGetVal(de_rock_hash);
        dictIterator *di_lrus = dictGetIterator(lrus);
        dictEntry *de_lrus;
        while ((de_lrus = dictNext(di_lrus)))
        {
            sds hash_field = dictGetKey(de_lrus);
            dictEntry *de_hash = dictFind(hash, hash_field);
            serverAssert(de_hash);
            serverAssert(dictGetKey(de_hash) == hash_field);
        }
        dictReleaseIterator(di_lrus);
    }
}

/* Before a key is deleted from redis db, it will call here.
 * NOTE: The key may be not a hash key
 */
void on_del_key_from_db_for_rock_hash(const int dbid, const sds redis_key)
{
    #if defined RED_ROCK_DEBUG
    debug_check_rock_hash(dbid, redis_key);
    #endif
    redisDb *db = server.db + dbid;
    dictDelete(db->rock_hash, redis_key);
}

/* After a key is overwritten in redis db, it will call here.
 * The new_o is the replaced object for the redis_key in db.
 */
void on_overwrite_key_from_db_for_rock_hash(const int dbid, const sds redis_key, const robj *new_o)
{
    redisDb *db = server.db + dbid;
    dictDelete(db->rock_hash, redis_key);

    if (server.hash_max_rock_entries == 0)
        return;

    if (!(new_o->type == OBJ_HASH && new_o->encoding == OBJ_ENCODING_HT))
        return;

    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);
    serverAssert(dictGetVal(de_db) == new_o);

    dict *hash = new_o->ptr;
    if (dictSize(hash) <= server.hash_max_rock_entries)
        return;

    // We need a the whole hash to rock hash
    sds internal_redis_key = dictGetKey(de_db);

    add_whole_redis_hash_to_rock_hash(dbid, internal_redis_key);
}

/* When flushdb or flushalldb, it will empty the db(s).
 * and we need reclaim the rock hash in the db.
 * if dbnum == -1, it means all db
 */
void on_empty_db_for_hash(const int dbnum)
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
        dict *rock_hash = db->rock_hash;
        dictEmpty(rock_hash, NULL);
    }
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
                    add_whole_redis_hash_to_rock_hash(i, internal_redis_key);
                }
            }
        }
        dictReleaseIterator(di);
    }
}