#ifndef __ROCK_H
#define __ROCK_H

#include "server.h"
#include <rocksdb/c.h>

// #define RED_ROCK_DEBUG      // run debug code if defined this macro. In release build, comment this line
#define RED_ROCK_EVICT_INFO     // for debug of eviction, please commont in release version

#define ROCK_KEY_FOR_DB     0
#define ROCK_KEY_FOR_HASH   1

void wait_rock_threads_exit();

// the global rocksdb handler
extern rocksdb_t* rockdb;   
extern redisAtomic int rock_threads_loop_forever;

void create_shared_object_for_rock();
void init_rocksdb(const char* folder_original_path);

sds encode_rock_key_for_db(const int dbid, sds redis_to_rock_key);
sds encode_rock_key_for_hash(const int dbid, sds hash_key_to_rock_key, const sds hash_field);
void decode_rock_key_for_db(const sds rock_key, int *dbid, const char **redis_key, size_t *key_sz);
void decode_rock_key_for_hash(const sds rock_key, int *dbid, 
                              const char **key, size_t *key_sz,
                              const char **field, size_t *field_sz);

void init_client_id_table();
client* lookup_client_from_id(const uint64_t client_id);
void on_add_a_new_client(client* const c);
void on_del_a_destroy_client(const client* const c);

void rock_evict(client *c);
void rock_evict_hash(client *c);

void debug_rock(client *c);
void rock_stat(client *c);

#define CHECK_EVICT_OK                                          0
#define CHECK_EVICT_EXPIRED                                     1
#define CHECK_EVICT_NOT_FOUND                                   2
#define CHECK_EVICT_ALREADY_WHOLE_ROCK_VALUE                    3
#define CHECK_EVICT_SHARED_VALUE                                4
#define CHECK_EVICT_NOT_SUPPORTED_TYPE                          5
#define CHECK_EVICT_IN_CANDIDAES                                6
// The following is special for key
#define CHECK_EVICT_ALREADY_IN_ROCK_HASH_FOR_DB_KEY             7
// The following is special for rock hash
#define CHECK_EVICT_TYPE_OR_ENCODING_WRONG_FOR_FIELD            8
#define CHECK_EVICT_NOT_FOUND_FIELD                             9
#define CHECK_EVICT_NOT_IN_ROCK_HASH_FOR_FIELD                  10   
#define CHECK_EVICT_ALREAY_FIELD_ROCK_VALUE                     11    

int check_valid_evict_of_key_for_db(const int dbid, const sds redis_key);
int check_valid_evict_of_key_for_hash(const int dbid, const sds hash_key, const sds hash_field);

// list* get_keys_in_rock_for_command(const client *c);

// int process_cmd_in_processInputBuffer(client *c);
#define CHECK_ROCK_GO_ON_TO_CALL    0
#define CHECK_ROCK_ASYNC_WAIT       1
#define CHECK_ROCK_CMD_FAIL         2
int check_and_set_rock_status_in_processCommand(client *c);

/* Check whether o is a rock value.
 * Return 1 if it is. Otherwise return 0.
 */
inline int is_rock_value(const robj *v)
{
    return  v == shared.rock_val_str_int ||
            v == shared.rock_val_str_other ||
            v == shared.rock_val_list_quicklist ||
            v == shared.rock_val_set_int ||
            v == shared.rock_val_set_ht ||
            v == shared.rock_val_hash_ziplist ||
            v == shared.rock_val_hash_ht ||
            v == shared.rock_val_zset_ziplist ||
            v == shared.rock_val_zset_skiplist;
}

/* Check whether o is a shared value which is made by makeObjectShared()
 * Return 1 if true. Otherwise return 0.
 */
inline int is_shared_value(const robj *v)
{
    return v->refcount == OBJ_SHARED_REFCOUNT;
}

/* Only OBJ_STRING (exclude OBJ_ENCODING_INT), OBJ_LIST, OBJ_SET, OBJ_HASH, OBJ_ZSET is included
 * and can be evicted to RocksDB.
 * Check get_match_rock_value() in rock_marshal.c
 * 
 * NOTE: other condition not check like shared object or rock value.
 */
inline int is_rock_type(const robj *o)
{
    return o->type == OBJ_STRING || 
        o->type == OBJ_LIST || 
        o->type == OBJ_SET ||
        o->type == OBJ_HASH ||
        o->type == OBJ_ZSET;
}

/* Stream and Module type is not supported for eviction to RocksDB.
 * return 1 if the value of v is not supported.
 * otherwise return 0. 
 */
inline int is_not_supported_evict_type(const robj *v)
{
    return !is_rock_type(v);
}

inline int is_client_in_waiting_rock_value_state(const client *c)
{
    return c->rock_key_num != 0;
}

// Redis Commands for Rock API

// generic API
list* generic_get_one_key_for_rock(const client *c, const int index);
list* generic_get_multi_keys_for_rock(const client *c, const int index, const int step);
list* generic_get_multi_keys_for_rock_exclude_tails(const client *c, const int index, 
                                                    const int step, const int tail_cnt);
list* generic_get_multi_keys_for_rock_in_range(const client *c, const int start, const int end);
list* generic_get_zset_num_for_rock(const client *c, const int have_dest);

void generic_get_one_field_for_rock(const client *c, const sds key, const int index,
                                    list **hash_keys, list **hash_fields);
void generic_get_all_fields_for_rock(const client *c, const sds key, list **hash_keys, list **hash_fields);
void generic_get_multi_fields_for_rock(const client *c, const sds key, const int index, const int step,
                                       list **hash_keys, list **hash_fields);

// string (t_string.c)
list* get_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* getex_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* getdel_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* append_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* strlen_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* setrange_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* getrange_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* incr_cmd_for_rock(const client* c, list **hash_keys, list **hash_fields);
list* decr_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* mget_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* incrby_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* decrby_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* incrbyfloat_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* getset_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* psetex_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* set_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* setex_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// bitop.c
list* setbit_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* getbit_cmd_for_rock(const client* c, list **hash_keys, list **hash_fields);
list* bitfield_cmd_for_rock(const client* c, list **hash_keys, list **hash_fields);
list* bitfield_ro_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* bitop_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* bitcount_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* bitpos_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// t_list.c
list* rpush_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* lpush_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* rpushx_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* lpushx_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* linsert_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* rpop_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* lpop_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* brpop_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* brpoplpush_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* blmoove_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* blpop_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* llen_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* lindex_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* lset_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* lrange_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* ltrim_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* lpos_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* lrem_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* rpoplpush_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* lmove_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// t_set.c
list* sadd_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* srem_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* smove_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* sismember_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* smismember_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* scard_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* spop_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* srandmember_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* sinter_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* sinterstore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* sunion_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* suionstore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* sdiff_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* sdiffstore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* smembers_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* sscan_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// t_zset.c
list* zadd_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zincrby_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrem_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zremrangebyscore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zremrangebyrank_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zremrangebylex_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zunionstore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zinterstore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zdiffstore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zunion_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zinter_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zdiff_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrange_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrangestore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrangebyscore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrevrangebyscore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrangebylex_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrevrangebylex_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zcount_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zlexcount_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrevrange_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zcard_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zscore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zmscore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrank_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrevrank_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zscan_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zpopmin_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zpopmax_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* bzpopmin_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* bzpopmax_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* zrandmember_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// t_hash.c
list* hset_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hsetnx_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hget_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hmset_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hmget_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hincrby_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hincrbyfloat_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hdel_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hlen_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hstrlen_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hkeys_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hvals_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hgetall_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hexists_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hrandfield_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hscan_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* hmset_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// db.c
list* move_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* copy_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* rename_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* renamenx_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// expire.c
list* expire_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* expireat_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* pexpire_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* pexpireat_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* ttl_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* touch_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* pttl_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* persist_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// multi.c
list* exec_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// sort.c
list* sort_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// debug.c
list* debug_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// cluster.c
list* migrate_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* dump_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// object.c
list* object_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// geo.c
list* geoadd_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* georadius_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* georadius_ro_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* georadiusbymember_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* georadiusbymember_ro_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* geohash_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* geopos_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* geodist_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* geosearch_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* geosearchstore_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

// hyperloglog.c
list* pfadd_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* pfcount_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* pfmerge_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);
list* pfdebug_cmd_for_rock(const client *c, list **hash_keys, list **hash_fields);

#endif
