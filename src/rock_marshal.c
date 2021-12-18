#include "rock_marshal.h"


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
#define ROCK_TYPE_ZSET_SKIPLIST     6 /* ZSET version 2 with doubles stored in binary. */
/* Object types for encoded objects. */
// #define ROCK_TYPE_HASH_ZIPMAP    9
// #define ROCK_TYPE_LIST_ZIPLIST  10
#define ROCK_TYPE_ZSET_ZIPLIST      12
#define ROCK_TYPE_HASH_ZIPLIST      13
// #define ROCK_TYPE_LIST_QUICKLIST    14
#define ROCK_TYPE_INVALID           127

// 1 byte for rock_type and 4 byte for LRU/LFU
#define MARSHAL_HEAD_SIZE (1 + sizeof(uint32_t))   


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

/* It is for memory optimization. 
 * We try our best to make enough room for a sds.
 * The frist byte is ROCK TYPE (see aboving).
 * The next 4-byte is for LRU/LFU information. 
 * in return sds and rock_type
 */
static sds create_sds_and_make_room(const robj* o, unsigned char *rock_type)
{
    sds s = sdsempty();
    s = sdsMakeRoomFor(s, MARSHAL_HEAD_SIZE);

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
        } 
        break;

    case OBJ_ZSET:
        if (o->encoding == OBJ_ENCODING_ZIPLIST) 
        {
        } 
        else if (o->encoding == OBJ_ENCODING_SKIPLIST) 
        {
        } 
        break;

    case OBJ_HASH:
        if (o->encoding == OBJ_ENCODING_ZIPLIST) 
        {
        } 
        else if (o->encoding == OBJ_ENCODING_HT) 
        {
        }
        break;

/*
    case OBJ_MODULE:
        break;

    case OBJ_STREAM:
        break;
*/

    default:
        serverPanic("create_sds_and_make_room(), unkkwon type = %d", o->type);
    }

    if (*rock_type == ROCK_TYPE_INVALID)
        serverPanic("create_sds_and_make_room(), rock_type invalid o->type = %d, o->encoding = %d",
                    o->type, o->encoding);

    // set type and LRU/LFU conetnet
    s = sdscatlen(s, rock_type, 1);
    uint32_t lru = o->lru;
    s = sdscatlen(s, &lru, sizeof(lru));
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

    default:
        serverPanic("marshal_object(), unknown rock_type = %d", (int)rock_type);
    }

    return s;
}


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

    default:
        serverPanic("unmarshal_object(), unknown rock_type = %d", (int)rock_type);
    }

    // set LRU/LFU
    const uint32_t lru = *((uint32_t*)(v+1));
    o->lru = lru;    

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

    default:
        serverPanic("debug_check_type(), unknown rock_type = %d", (int)rock_type);
    }

    return 0;
}

