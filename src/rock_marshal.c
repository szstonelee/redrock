#include "rock_marshal.h"
#include "server.h"
#include "rock.h"

// #include "assert.h"

/* Map object types to Rocksdb object types. Macros starting with OBJ_ are for
 * memory storage and may change. Instead Rocksdb types must be fixed because
 * we store them on disk. */
#define ROCK_TYPE_STRING_INT        0
#define ROCK_TYPE_STRING_OTHER      1       // OBJ_ENCODING_RAW or OBJ_ENCODING_EMBSTR
#define ROCK_TYPE_LIST              2       // List can only be encoded as quicklist
#define ROCK_TYPE_SET_INT           3
#define ROCK_TYPE_SET_HT            4
#define ROCK_TYPE_HASH_HT           5
#define ROCK_TYPE_HASH_ZIPLIST      6
#define ROCK_TYPE_ZSET_ZIPLIST      7
#define ROCK_TYPE_ZSET_SKIPLIST     8

#define ROCK_TYPE_INVALID           127

// 1 byte for rock_type and 4 byte for LRU/LFU
// #define MARSHAL_HEAD_SIZE (1 + sizeof(uint32_t))  
// Now we do not need to marshal LRU/LFU info
#define MARSHAL_HEAD_SIZE 1 


/* From the input o, determine which shared object is right 
 * for replacing it.
 */
robj* get_match_rock_value(const robj *o)
{
    robj *match = NULL;

    switch(o->type)
    {
    case OBJ_STRING:        
        if (o->encoding == OBJ_ENCODING_INT)
        {
            // NOTE: OBJ_ENCODING_INT is OK for RocksDB because it use one robj size
            match = shared.rock_val_str_int;
        }
        else if (o->encoding == OBJ_ENCODING_RAW || o->encoding == OBJ_ENCODING_EMBSTR)
        {
            match = shared.rock_val_str_other;
        }
        break;

    case OBJ_LIST:
        if (o->encoding == OBJ_ENCODING_QUICKLIST)
            match = shared.rock_val_list_quicklist;

        break;

    case OBJ_SET:
        if (o->encoding == OBJ_ENCODING_INTSET)
        {
            match = shared.rock_val_set_int;
        }
        else if (o->encoding == OBJ_ENCODING_HT)
        {
            match = shared.rock_val_set_ht;
        }
        break;

    case OBJ_HASH:
        if (o->encoding == OBJ_ENCODING_HT)
        {
            match = shared.rock_val_hash_ht;
        } 
        else if (o->encoding == OBJ_ENCODING_ZIPLIST)
        {
            match = shared.rock_val_hash_ziplist;
        }
        break;

    case OBJ_ZSET:
        if (o->encoding == OBJ_ENCODING_ZIPLIST)
        {
            match = shared.rock_val_zset_ziplist;
        }
        else if (o->encoding == OBJ_ENCODING_SKIPLIST)
        {
            match = shared.rock_val_zset_skiplist;
        }
        break;

    default:
        serverPanic("get_match_rock_value() unmatched!");
    }

    serverAssert(match != NULL);
    return match;
}

static sds marshal_str_int(const robj *o, sds s)
{
    long long val = (long long)o->ptr;
    s = sdscatlen(s, &val, 8);
    return s;
}


static size_t cal_room_str_int(const robj *o)
{
    UNUSED(o);
    return 8;
}

static robj* unmarshal_str_int(const char *buf, const size_t sz)
{
    serverAssert(sz == 8);
    long long val = *((long long*)buf);
    return createStringObjectFromLongLong(val);
}

static sds marshal_str_other(const robj *o, sds s)
{
    s = sdscatlen(s, o->ptr, sdslen(o->ptr));
    return s;
}

static size_t cal_room_str_other(const robj *o)
{
    return sdslen(o->ptr);
}

static robj* unmarshal_str_other(const char *buf, const size_t sz)
{
    return createStringObject(buf, sz);
}

/* We use AOF way. In futrue, maybe change to RDB way 
 * And we assume the total size for one entry in quick list is less than 4G (unsigned int) */
static sds marshal_list(const robj *o, sds s)
{
    quicklist *ql = o->ptr;    

    quicklistIter *qit = quicklistGetIterator(ql, AL_START_HEAD);
    quicklistEntry entry;

    while(quicklistNext(qit, &entry)) 
    {
        if (entry.value) 
        {
            unsigned int len = entry.sz;
            s = sdscatlen(s, &len, sizeof(unsigned int));
            s = sdscatlen(s, entry.value, len);
        } 
        else 
        {
            sds str = sdsfromlonglong(entry.longval);
            serverAssert(sdslen(str) <= UINT_MAX);
            unsigned int len = (unsigned int)sdslen(str);
            s = sdscatlen(s, &len, sizeof(unsigned int));
            s = sdscatlen(s, str, len);
            sdsfree(str);
        }
    }

    quicklistReleaseIterator(qit);

    return s;
}

static size_t cal_room_list(const robj *o)
{
    size_t room = 0;

    quicklist *ql = o->ptr;    

    quicklistIter *qit = quicklistGetIterator(ql, AL_START_HEAD);
    quicklistEntry entry;

    while(quicklistNext(qit, &entry)) 
    {
        if (entry.value) 
        {
            unsigned int len = entry.sz;            
            room += sizeof(unsigned int);
            room += len;
        } 
        else 
        {
            sds str = sdsfromlonglong(entry.longval);
            unsigned int len = (unsigned int)sdslen(str);
            room += sizeof(unsigned int);
            room += len;
            sdsfree(str);
        }
    }

    quicklistReleaseIterator(qit);

    return room;    
}

static robj* unmarshal_list(const char *buf, const size_t sz)
{
    robj *list = createQuicklistObject();
    quicklistSetOptions(list->ptr, server.list_max_ziplist_size,
                        server.list_compress_depth);

    char *s = (char*)buf;
    long long len = sz;
    while (len > 0) 
    {
        unsigned int entry_len = *((unsigned int*)s);
        s += sizeof(unsigned int);
        len -= sizeof(unsigned int);

        quicklistPushTail(list->ptr, s, entry_len);

        s += entry_len;
        len -= entry_len;
    }
    serverAssert(len == 0);

    return list;
}

static sds marshal_set_int(const robj *o, sds s)
{
    intset *is = o->ptr;
    s = sdscatlen(s, &(is->encoding), sizeof(uint32_t));
    s = sdscatlen(s, &(is->length), sizeof(uint32_t));
    size_t content_len = is->encoding * is->length;
    s = sdscatlen(s, is->contents, content_len);

    return s;
}

static size_t cal_room_set_int(const robj *o)
{
    size_t room = 0;

    intset *is = o->ptr;
    room += sizeof(uint32_t);
    room += sizeof(uint32_t);
    size_t content_len = is->encoding * is->length;
    room += content_len;

    return room;
}

static robj* unmarshal_set_int(const char *buf, const size_t sz)
{
    long long len = sz;

    char *s = (char*)buf;
    robj *o = createIntsetObject();
    intset *is = (intset*)(o->ptr);

    uint32_t is_encoding = *((uint32_t*)s);
    is->encoding = is_encoding;
    s += sizeof(is_encoding);
    len -= sizeof(is_encoding);

    uint32_t is_length = *((uint32_t*)s);
    is->length = is_length;
    s += sizeof(is_length);
    len -= sizeof(is_length);

    /* reference intset.c intsetResize() */
    size_t content_sz = is_length * is_encoding;
    len -= content_sz;
    serverAssert(len == 0);
    is = zrealloc(is, sizeof(intset)+content_sz);
    o->ptr = is;
    memcpy(is->contents, s, content_sz);
    
    return o;
}

static sds marshal_set_ht(const robj *o, sds s)
{
    dict *set = o->ptr;
    size_t count = dictSize(set);
    s = sdscatlen(s, &count, sizeof(size_t));

    dictIterator *di = dictGetIterator(set);
    dictEntry* de;
    while ((de = dictNext(di))) 
    {
        sds ele = dictGetKey(de);
        size_t ele_len = sdslen(ele);
        s = sdscatlen(s, &ele_len, sizeof(size_t));
        s = sdscatlen(s, ele, ele_len);
    }
    dictReleaseIterator(di);

    return s;
}

static size_t cal_room_set_ht(const robj *o)
{
    dict *set = o->ptr;
    size_t room = 0;

    room += sizeof(size_t);     // for above count

    dictIterator *di = dictGetIterator(set);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        sds ele = dictGetKey(de);
        size_t ele_len = sdslen(ele);
        room += sizeof(size_t);
        room += ele_len;
    }
    dictReleaseIterator(di);

    return room;    
}

static robj* unmarshal_set_ht(const char *buf, const size_t sz)
{
    char *s = (char*)buf;
    long long len = sz;

    size_t count = *((size_t*)s);
    s += sizeof(size_t);
    len -= sizeof(size_t);

    robj *o = createSetObject();
    if (count > DICT_HT_INITIAL_SIZE)
        dictExpand(o->ptr, count);

    while (count--) 
    {
        size_t ele_len;
        ele_len = *((size_t*)s);
        s += sizeof(size_t);
        len -= sizeof(size_t);

            
        sds sdsele = sdsnewlen(s, ele_len);
        dictAdd(o->ptr, sdsele, NULL);
        s += ele_len;
        len -= ele_len;
    }

    serverAssert(len == 0);
    return o;
}

static sds marshal_hash_ht(const robj *o, sds s)
{
    dict *hash = o->ptr;
    size_t count = dictSize(hash);
    s = sdscatlen(s, &count, sizeof(size_t));

    dictIterator *di = dictGetIterator(hash);
    dictEntry *de;
    while ((de = dictNext(di))) 
    {
        sds field = dictGetKey(de);            
        size_t field_len = sdslen(field);            
        s = sdscatlen(s, &field_len, sizeof(size_t));
        s = sdscatlen(s, field, field_len);

        sds val = dictGetVal(de);
        size_t val_len = sdslen(val);
        s = sdscatlen(s, &val_len, sizeof(size_t));
        s = sdscatlen(s, val, val_len);
    }
    dictReleaseIterator(di); 

    return s;
}

static size_t cal_room_hash_ht(const robj *o)
{
    dict *hash = o->ptr;
    size_t room = 0;

    room += sizeof(size_t);     // for above count

    dictIterator *di = dictGetIterator(hash);
    dictEntry* de;
    while ((de = dictNext(di))) 
    {
        sds field = dictGetKey(de);            
        size_t field_len = sdslen(field);            
        room += sizeof(size_t);
        room += field_len;

        sds val = dictGetVal(de);
        size_t val_len = sdslen(val);
        room += sizeof(size_t);
        room += val_len;
    }
    dictReleaseIterator(di);

    return room;
}

static robj* unmarshal_hash_ht(const char *buf, const size_t sz)
{
    char *s = (char*)buf;
    long long len = sz;

    size_t count = *((size_t*)s);
    s += sizeof(size_t);
    len -= sizeof(size_t);

    dict *dict_internal = dictCreate(&hashDictType, NULL);
    if (count > DICT_HT_INITIAL_SIZE)
        dictExpand(dict_internal, count);

    while (count--)
    {
        size_t field_len = *((size_t*)s);
        s += sizeof(size_t);
        len -= sizeof(size_t);
        sds field = sdsnewlen(s, field_len);
        s += field_len;
        len -= field_len;

        size_t val_len = *((size_t*)s);
        s += sizeof(size_t);
        len -= sizeof(size_t);
        sds val = sdsnewlen(s, val_len);
        s += val_len;
        len -= val_len;

        int ret = dictAdd(dict_internal, field, val);
        serverAssert(ret == DICT_OK);
    }

    serverAssert(len == 0);

    robj *o = createObject(OBJ_HASH, dict_internal);
    o->encoding = OBJ_ENCODING_HT;

    return o;
}

static sds marshal_hash_ziplist(const robj *o, sds s)
{
    size_t zip_list_bytes_len = ziplistBlobLen((unsigned char*)o->ptr);
    s = sdscatlen(s, &zip_list_bytes_len, sizeof(size_t));
    s = sdscatlen(s, o->ptr, zip_list_bytes_len);

    return s;
}

static size_t cal_room_hash_ziplist(const robj *o)
{
    size_t room = 0;

    room += sizeof(size_t);
    size_t zip_list_bytes_len = ziplistBlobLen((unsigned char*)o->ptr);
    room += zip_list_bytes_len;

    return room;
}

static robj* unmarshal_hash_ziplist(const char *buf, const size_t sz)
{
    unsigned char *s = (unsigned char*)buf;
    long long len = sz;

    size_t zip_list_bytes_len = *((size_t*)s);
    s += sizeof(size_t);
    len -= sizeof(size_t);

    #if defined RED_ROCK_DEBUG
    serverAssert(hashZiplistValidateIntegrity(s, zip_list_bytes_len, 1));
    #endif

    unsigned char *ziplist = zmalloc(zip_list_bytes_len);
    memcpy(ziplist, s, zip_list_bytes_len);
    s += zip_list_bytes_len;
    len -= zip_list_bytes_len;

    serverAssert(len == 0);

    // robj *o = createObject(OBJ_HASH, ziplist);
    robj *o = createObject(OBJ_STRING, ziplist);
    o->type = OBJ_HASH;
    o->encoding = OBJ_ENCODING_ZIPLIST;

    return o;
}

static sds marshal_zset_ziplist(const robj *o, sds s)
{
    size_t zip_list_bytes_len = ziplistBlobLen((unsigned char*)o->ptr);
    s = sdscatlen(s, &zip_list_bytes_len, sizeof(size_t));
    s = sdscatlen(s, o->ptr, zip_list_bytes_len);

    return s;
}

static size_t cal_room_zset_ziplist(const robj *o)
{
    size_t room = 0;

    room += sizeof(size_t);
    size_t zip_list_bytes_len = ziplistBlobLen((unsigned char*)o->ptr);
    room += zip_list_bytes_len;

    return room;
}

static robj* unmarshal_zset_ziplist(const char *buf, const size_t sz)
{
    unsigned char *s = (unsigned char*)buf;
    long long len = sz;

    size_t zip_list_bytes_len = *((size_t*)s);
    s += sizeof(size_t);
    len -= sizeof(size_t);

    #if defined RED_ROCK_DEBUG
    serverAssert(hashZiplistValidateIntegrity(s, zip_list_bytes_len, 1));
    #endif

    char *ziplist = zmalloc(zip_list_bytes_len);
    memcpy(ziplist, s, zip_list_bytes_len);
    s += zip_list_bytes_len;
    len -= zip_list_bytes_len;
    serverAssert(len == 0);

    // robj *o = createObject(OBJ_ZSET, ziplist);
    robj *o = createObject(OBJ_STRING, ziplist);
    o->type = OBJ_ZSET;
    o->encoding = OBJ_ENCODING_ZIPLIST;

    return o;
}

static sds marshal_zset_skiplist(const robj *o, sds s)
{
    zset *zs = o->ptr;
    zskiplist *zsl = zs->zsl;
    uint64_t zsl_length = zsl->length;
    s = sdscatlen(s, &zsl_length, sizeof(uint64_t));

    zskiplistNode *zn = zsl->tail;
    while (zn != NULL) 
    {
        sds ele = zn->ele;
        size_t ele_len = sdslen(ele);
        s = sdscatlen(s, &ele_len, sizeof(size_t));
        s = sdscatlen(s, ele, ele_len);
        double score = zn->score;
        s = sdscatlen(s, &score, sizeof(double));

        zn = zn->backward;
    }

    return s;
}

static size_t cal_room_zset_skiplist(const robj *o)
{
    size_t room = 0;

    zset *zs = o->ptr;
    zskiplist *zsl = zs->zsl;
    room += sizeof(uint64_t);

    zskiplistNode *zn = zsl->tail;
    while (zn != NULL)
    {
        sds ele = zn->ele;
        size_t ele_len = sdslen(ele);
        room += sizeof(size_t);
        room += ele_len;

        room += sizeof(double);

        zn = zn->backward;
    }

    return room;
}

static robj* unmarshal_zset_skiplist(const char *buf, const size_t sz)
{
    char *s = (char*)buf;
    long long len = sz;

    uint64_t zset_len = *((uint64_t*)s);
    s += sizeof(uint64_t);
    len -= sizeof(uint64_t);
        
    robj *o = createZsetObject();
    zset *zs = o->ptr;

    if (zset_len > DICT_HT_INITIAL_SIZE)
        dictExpand(zs->dict, zset_len);
        
    while (zset_len--) {
        sds sdsele;
        double score;
        zskiplistNode *znode;

        size_t ele_len = *((size_t*)s);
        s += sizeof(size_t);
        len -= sizeof(size_t);
        sdsele = sdsnewlen(s, ele_len);
        s += ele_len;
        len -= ele_len;

        score = *((double*)s);
        s += sizeof(double);
        len -= sizeof(double);

        znode = zslInsert(zs->zsl, score, sdsele);
        int ret = dictAdd(zs->dict, sdsele, &znode->score);
        serverAssert(ret == DICT_OK);
    }

    serverAssert(len == 0);
    return o;
}

/* It is for memory optimization. 
 * We try our best to make enough room for a sds.
 * The frist byte is ROCK TYPE (see aboving).
 * The next 4-byte is for LRU/LFU information in return sds 
 * rock_type wil be set the coresponding type, ROCK_TYPE_XXX.
 */
static sds create_sds_and_make_room(const robj* o, unsigned char *rock_type)
{
    sds s = sdsempty();
    
    *rock_type = ROCK_TYPE_INVALID;
    size_t obj_room = 0;
    switch (o->type) 
    {
    case OBJ_STRING:
        if (o->encoding == OBJ_ENCODING_INT)
        {
            *rock_type = ROCK_TYPE_STRING_INT;
            obj_room = cal_room_str_int(o);
        }
        else if (o->encoding == OBJ_ENCODING_RAW || o->encoding == OBJ_ENCODING_EMBSTR)
        {
            *rock_type = ROCK_TYPE_STRING_OTHER;
            obj_room = cal_room_str_other(o);
        }        
        break;

    case OBJ_LIST:
        if (o->encoding == OBJ_ENCODING_QUICKLIST) 
        {
            *rock_type = ROCK_TYPE_LIST;
            obj_room = cal_room_list(o);
        }
        break;

    case OBJ_SET:
        if (o->encoding == OBJ_ENCODING_INTSET) 
        {
            *rock_type = ROCK_TYPE_SET_INT;
            obj_room = cal_room_set_int(o);
        } 
        else if (o->encoding == OBJ_ENCODING_HT) 
        {
            *rock_type = ROCK_TYPE_SET_HT;
            obj_room = cal_room_set_ht(o);
        } 
        break;

    case OBJ_HASH:
        if (o->encoding == OBJ_ENCODING_ZIPLIST) 
        {
            *rock_type = ROCK_TYPE_HASH_ZIPLIST;
            obj_room = cal_room_hash_ziplist(o);
        } 
        else if (o->encoding == OBJ_ENCODING_HT) 
        {
            *rock_type = ROCK_TYPE_HASH_HT;
            obj_room = cal_room_hash_ht(o);
        }
        break;

    case OBJ_ZSET:
        if (o->encoding == OBJ_ENCODING_ZIPLIST) 
        {
            *rock_type = ROCK_TYPE_ZSET_ZIPLIST;
            obj_room = cal_room_zset_ziplist(o);
        } 
        else if (o->encoding == OBJ_ENCODING_SKIPLIST) 
        {
            *rock_type = ROCK_TYPE_ZSET_SKIPLIST;
            obj_room = cal_room_zset_skiplist(o);
        } 
        break;

    default:
        serverPanic("create_sds_and_make_room(), unkkwon type = %d", o->type);
        break;
    }

    if (*rock_type == ROCK_TYPE_INVALID)
        serverPanic("create_sds_and_make_room(), rock_type invalid o->type = %d, o->encoding = %d",
                    o->type, o->encoding);

    s = sdsMakeRoomFor(s, MARSHAL_HEAD_SIZE);
    // set type and LRU/LFU conetnet
    s = sdscatlen(s, rock_type, 1);
    // NOTE: We do not need to save LRU info in RocksDB
    //       When object is restored from DB, the default time in createObject()
    //       is OK because for LRU, it is the current time, 
    //       for LFU, it is default counter 5 (LFU_INIT_VAL) 
    //       Check createObject() in object.c for help
    // uint32_t lru = o->lru;      // the redis object's lru only use 24 bit information
    // s = sdscatlen(s, &lru, sizeof(uint32_t));
    // make room for object
    s = sdsMakeRoomFor(s, obj_room);

    return s;
}

/* Serialization from dbid and o of robj.
 * It will allocate memory for the return value.
 */
sds marshal_object(const robj* o)
{
    unsigned char rock_type;
    sds s = create_sds_and_make_room(o, &rock_type);
    serverAssert(sdslen(s) >= MARSHAL_HEAD_SIZE);     // at least 5 byte

    switch(rock_type)
    {
    case ROCK_TYPE_STRING_INT:
        s = marshal_str_int(o, s);
        break;
        
    case ROCK_TYPE_STRING_OTHER:
        s = marshal_str_other(o, s);
        break;

    case ROCK_TYPE_LIST:
        s = marshal_list(o, s);
        break;

    case ROCK_TYPE_SET_INT:
        s = marshal_set_int(o, s);
        break;

    case ROCK_TYPE_SET_HT:
        s = marshal_set_ht(o, s);
        break;

    case ROCK_TYPE_HASH_HT:
        s = marshal_hash_ht(o, s);
        break;

    case ROCK_TYPE_HASH_ZIPLIST:
        s = marshal_hash_ziplist(o, s);
        break;

    case ROCK_TYPE_ZSET_ZIPLIST:
        s = marshal_zset_ziplist(o, s);
        break;

    case ROCK_TYPE_ZSET_SKIPLIST:
        s = marshal_zset_skiplist(o, s);
        break;

    default:
        serverPanic("marshal_object(), unknown rock_type = %d", (int)rock_type);
    }

    return s;
}

/* Must sucess, otherwise assert fail */
robj* unmarshal_object(const sds v)
{
    serverAssert(sdslen(v) >= MARSHAL_HEAD_SIZE);
    size_t sz = sdslen(v) - MARSHAL_HEAD_SIZE;
    const char *buf = v + MARSHAL_HEAD_SIZE;

    const unsigned char rock_type = v[0];
    robj *o = NULL;

    switch(rock_type)
    {
    case ROCK_TYPE_STRING_INT:
        o = unmarshal_str_int(buf, sz);
        break;

    case ROCK_TYPE_STRING_OTHER:
        o = unmarshal_str_other(buf, sz);
        break;

    case ROCK_TYPE_LIST:
        o = unmarshal_list(buf, sz);
        break;

    case ROCK_TYPE_SET_INT:
        o = unmarshal_set_int(buf, sz);
        break;

    case ROCK_TYPE_SET_HT:
        o = unmarshal_set_ht(buf, sz);
        break;

    case ROCK_TYPE_HASH_HT:
        o = unmarshal_hash_ht(buf, sz);
        break;

    case ROCK_TYPE_HASH_ZIPLIST:
        o = unmarshal_hash_ziplist(buf, sz);
        break;

    case ROCK_TYPE_ZSET_ZIPLIST:
        o = unmarshal_zset_ziplist(buf, sz);
        break;

    case ROCK_TYPE_ZSET_SKIPLIST:
        o = unmarshal_zset_skiplist(buf, sz);
        break;

    default:
        serverPanic("unmarshal_object(), unknown rock_type = %d", (int)rock_type);
    }

    // NOT: do not need LRU/LFU, because the createObject() default LRU/LFU is OK
    // const uint32_t lru = *((uint32_t*)(v+1));
    // o->lru = lru;    

    return o;
}

/* Check type match */
int debug_check_type(const sds recover_val, const robj *shared_obj)
{
    serverAssert(sdslen(recover_val) >= 1);
    
    unsigned char rock_type = recover_val[0];

    switch(rock_type)
    {
    case ROCK_TYPE_STRING_INT:
        if (shared_obj == shared.rock_val_str_int)
            return 1;

        break;

    case ROCK_TYPE_STRING_OTHER:
        if (shared_obj == shared.rock_val_str_other)
            return 1;

        break;

    case ROCK_TYPE_LIST:
        if (shared_obj == shared.rock_val_list_quicklist)
            return 1;
        
        break;

    case ROCK_TYPE_SET_INT:
        if (shared_obj == shared.rock_val_set_int)
            return 1;
        
        break;

    case ROCK_TYPE_SET_HT:
        if (shared_obj == shared.rock_val_set_ht)
            return 1;

        break;

    case ROCK_TYPE_HASH_HT:
        if (shared_obj == shared.rock_val_hash_ht)
            return 1;

        break;

    case ROCK_TYPE_HASH_ZIPLIST:
        if (shared_obj == shared.rock_val_hash_ziplist)
            return 1;

        break;

    case ROCK_TYPE_ZSET_ZIPLIST:
        if (shared_obj == shared.rock_val_zset_ziplist)
            return 1;

        break;

    case ROCK_TYPE_ZSET_SKIPLIST:
        if (shared_obj == shared.rock_val_zset_skiplist)
            return 1;

        break;

    default:
        serverPanic("debug_check_type(), unknown rock_type = %d", (int)rock_type);
    }

    return 0;
}

