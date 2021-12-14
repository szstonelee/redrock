#include "rock_marshal.h"


// #include "assert.h"

/* Map object types to Rocksdb object types. Macros starting with OBJ_ are for
 * memory storage and may change. Instead Rocksdb types must be fixed because
 * we store them on disk. */
#define ROCK_TYPE_STRING_INT        0
#define ROCK_TYPE_STRING_OTHER      1    // OBJ_ENCODING_RAW or OBJ_ENCODING_EMBSTR
// #define ROCK_TYPE_LIST   1
#define ROCK_TYPE_SET_HT            2
// #define ROCK_TYPE_ZSET   3
#define ROCK_TYPE_HASH_HT           4
#define ROCK_TYPE_ZSET_SKIPLIST 5 /* ZSET version 2 with doubles stored in binary. */
/* Object types for encoded objects. */
// #define ROCK_TYPE_HASH_ZIPMAP    9
// #define ROCK_TYPE_LIST_ZIPLIST  10
#define ROCK_TYPE_SET_INTSET        11
#define ROCK_TYPE_ZSET_ZIPLIST      12
#define ROCK_TYPE_HASH_ZIPLIST      13
#define ROCK_TYPE_LIST_QUICKLIST    14
#define ROCK_TYPE_INVALID           127


/* It is for memory optimization. 
 * We try our best to make enough room for a sds.
 * The frist byte is ROCK TYPE (see aboving) in return sds and rock_type
 */
static sds create_sds_and_make_room(const robj* o, unsigned char *rock_type)
{
    sds s = sdsempty();

    *rock_type = ROCK_TYPE_INVALID;
    switch (o->type) 
    {
    case OBJ_STRING:
        if (o->encoding == OBJ_ENCODING_INT)
        {
            *rock_type = ROCK_TYPE_STRING_INT;
            s = sdscatlen(s, rock_type, 1);
            s = sdsMakeRoomFor(s, 8);
        }
        else if (o->encoding == OBJ_ENCODING_RAW || o->encoding == OBJ_ENCODING_EMBSTR)
        {
            *rock_type = ROCK_TYPE_STRING_OTHER;
            s = sdscatlen(s, rock_type, 1);
            s = sdsMakeRoomFor(s, sdslen(o->ptr));
        }        
        break;

    case OBJ_LIST:
        if (o->encoding == OBJ_ENCODING_QUICKLIST) 
        {
        } 
        break;

    case OBJ_SET:
        if (o->encoding == OBJ_ENCODING_INTSET) 
        {
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

    case OBJ_MODULE:
        break;

    case OBJ_STREAM:
        break;

    default:
        serverPanic("create_sds_and_make_room(), unkkwon type = %d", o->type);
    }

    if (*rock_type == ROCK_TYPE_INVALID)
        serverPanic("create_sds_and_make_room(), rock_type invalid o->type = %d, o->encoding = %d",
                    o->type, o->encoding);

    return s;
}

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

static robj* unmarshal_str_int(const char *buf, size_t sz)
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

static robj* unmarshal_str_other(const char *buf, size_t sz)
{
    return createStringObject(buf, sz);
}

/* Serialization from dbid and o of robj.
 * It will allocate memory for the return value.
 */
sds marshal_object(const robj* o)
{
    unsigned char rock_type;
    sds s = create_sds_and_make_room(o, &rock_type);
    serverAssert(sdslen(s) >= 1);     // at least one byte for rock type

    switch(rock_type)
    {
    case ROCK_TYPE_STRING_INT:
        s = marshal_str_int(o, s);
        break;
    case ROCK_TYPE_STRING_OTHER:
        s = marshal_str_other(o, s);
        break;
    default:
        serverPanic("marshal_object(), unknown rock_type = %d", (int)rock_type);
    }

    return s;
}


robj* unmarshal_object(const sds s)
{
    serverAssert(sdslen(s) >= 1);
    size_t sz = sdslen(s) - 1;
    const char *buf = s + 1;

    unsigned char rock_type = s[0];
    robj *o = NULL;

    switch(rock_type)
    {
    case ROCK_TYPE_STRING_INT:
        o = unmarshal_str_int(buf, sz);
        break;
    case ROCK_TYPE_STRING_OTHER:
        o = unmarshal_str_other(buf, sz);
        break;
    default:
        serverPanic("unmarshal_object(), unknown rock_type = %d", (int)rock_type);
    }

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

    default:
        serverPanic("debug_check_type(), unknown rock_type = %d", (int)rock_type);
    }

    return 0;
}

