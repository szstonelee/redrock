#ifndef __ROCK_H
#define __ROCK_H

#include "server.h"
#include <rocksdb/c.h>

#define RED_ROCK_DEBUG      // run debug code if defined this macro. In release build, comment this line

void wait_rock_threads_exit();

// the global rocksdb handler
extern rocksdb_t* rockdb;   
extern redisAtomic int rock_threads_loop_forever;

void create_shared_object_for_rock();
void init_rocksdb(const char* folder_original_path);
void debug_rock(client *c);
sds encode_rock_key(const int dbid, sds redis_to_rock_key);
void decode_rock_key(const sds rock_key, int* dbid, char** redis_key, size_t* key_sz);

void init_client_id_table();
client* lookup_client_from_id(const uint64_t client_id);
void on_add_a_new_client(client* const c);
void on_del_a_destroy_client(const client* const c);

void rock_evict(client *c);

// list* get_keys_in_rock_for_command(const client *c);

// int process_cmd_in_processInputBuffer(client *c);
int check_and_set_rock_status_in_processCommand(client *c);


/* Check whether o is a rock value.
 * Return 1 if it is. Otherwise return 0.
 */
inline int is_rock_value(const robj *v)
{
    return  v == shared.rock_val_str_other ||
            v == shared.rock_val_str_int ||
            v == shared.rock_val_list_quicklist;
}

/* Check whether o is a shared value which is made by makeObjectShared()
 * Return 1 if true. Otherwise return 0.
 */
inline int is_shared_value(const robj *v)
{
    return v->refcount == OBJ_SHARED_REFCOUNT;
}

/* Check whether the value can be evicted. 
 * Return 1 if can, otherwise, return 0.
 *
 * We exclude such cases:
 * 1. already rock value
 * 2. already shared value
 * 3. value type not suppoorted, right now, it is OBJ_STREAM  
 */
inline int is_evict_value(const robj *v)
{
    if (is_rock_value(v))
    {
        return 0;
    }
    else if (is_shared_value(v))
    {
        return 0;
    }
    else if (v->type == OBJ_STREAM)
    {
        return 0;
    }
    else
    {
        serverAssert(v->type != OBJ_MODULE);
        return 1;
    }
}

/* Check a client is in the state waiting for rock value.
 * Return 1 if the client is waiting some rock value.
 * Otherwise return 0.
 */
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

// string (t_string.c)
list* get_cmd_for_rock(const client *c);
list* getex_cmd_for_rock(const client *c);
list* getdel_cmd_for_rock(const client *c);
list* append_cmd_for_rock(const client *c);
list* strlen_cmd_for_rock(const client *c);
list* setrange_cmd_for_rock(const client *c);
list* getrange_cmd_for_rock(const client *c);
list* incr_cmd_for_rock(const client* c);
list* decr_cmd_for_rock(const client *c);
list* mget_cmd_for_rock(const client *c);
list* incrby_cmd_for_rock(const client *c);
list* decrby_cmd_for_rock(const client *c);
list* incrbyfloat_cmd_for_rock(const client *c);
list* getset_cmd_for_rock(const client *c);

// bitop.c
list* setbit_cmd_for_rock(const client *c);
list* getbit_cmd_for_rock(const client* c);
list* bitfield_cmd_for_rock(const client* c);
list* bitfield_ro_cmd_for_rock(const client *c);
list* bitop_cmd_for_rock(const client *c);
list* bitcount_cmd_for_rock(const client *c);
list* bitpos_cmd_for_rock(const client *c);

// t_list.c
list* rpush_cmd_for_rock(const client *c);
list* lpush_cmd_for_rock(const client *c);
list* rpushx_cmd_for_rock(const client *c);
list* lpushx_cmd_for_rock(const client *c);
list* linsert_cmd_for_rock(const client *c);
list* rpop_cmd_for_rock(const client *c);
list* lpop_cmd_for_rock(const client *c);
list* brpop_cmd_for_rock(const client *c);
list* brpoplpush_cmd_for_rock(const client *c);
list* blmoove_cmd_for_rock(const client *c);
list* blpop_cmd_for_rock(const client *c);
list* llen_cmd_for_rock(const client *c);
list* lindex_cmd_for_rock(const client *c);
list* lset_cmd_for_rock(const client *c);
list* lrange_cmd_for_rock(const client *c);
list* ltrim_cmd_for_rock(const client *c);
list* lpos_cmd_for_rock(const client *c);
list* lrem_cmd_for_rock(const client *c);
list* rpoplpush_cmd_for_rock(const client *c);
list* lmove_cmd_for_rock(const client *c);

// t_set.c
list* sadd_cmd_for_rock(const client *c);
list* srem_cmd_for_rock(const client *c);
list* smove_cmd_for_rock(const client *c);
list* sismember_cmd_for_rock(const client *c);
list* smismember_cmd_for_rock(const client *c);
list* scard_cmd_for_rock(const client *c);
list* spop_cmd_for_rock(const client *c);
list* srandmember_cmd_for_rock(const client *c);
list* sinter_cmd_for_rock(const client *c);
list* sinterstore_cmd_for_rock(const client *c);
list* sunion_cmd_for_rock(const client *c);
list* suionstore_cmd_for_rock(const client *c);
list* sdiff_cmd_for_rock(const client *c);
list* sdiffstore_cmd_for_rock(const client *c);
list* smembers_cmd_for_rock(const client *c);
list* sscan_cmd_for_rock(const client *c);

// t_zset.c
list* zadd_cmd_for_rock(const client *c);
list* zincrby_cmd_for_rock(const client *c);
list* zrem_cmd_for_rock(const client *c);
list* zremrangebyscore_cmd_for_rock(const client *c);
list* zremrangebyrank_cmd_for_rock(const client *c);
list* zremrangebylex_cmd_for_rock(const client *c);
list* zunionstore_cmd_for_rock(const client *c);
list* zinterstore_cmd_for_rock(const client *c);
list* zdiffstore_cmd_for_rock(const client *c);
list* zunion_cmd_for_rock(const client *c);
list* zinter_cmd_for_rock(const client *c);
list* zdiff_cmd_for_rock(const client *c);
list* zrange_cmd_for_rock(const client *c);
list* zrangestore_cmd_for_rock(const client *c);
list* zrangebyscore_cmd_for_rock(const client *c);
list* zrevrangebyscore_cmd_for_rock(const client *c);
list* zrangebylex_cmd_for_rock(const client *c);
list* zrevrangebylex_cmd_for_rock(const client *c);
list* zcount_cmd_for_rock(const client *c);
list* zlexcount_cmd_for_rock(const client *c);
list* zrevrange_cmd_for_rock(const client *c);
list* zcard_cmd_for_rock(const client *c);
list* zscore_cmd_for_rock(const client *c);
list* zmscore_cmd_for_rock(const client *c);
list* zrank_cmd_for_rock(const client *c);
list* zrevrank_cmd_for_rock(const client *c);
list* zscan_cmd_for_rock(const client *c);
list* zpopmin_cmd_for_rock(const client *c);
list* zpopmax_cmd_for_rock(const client *c);
list* bzpopmin_cmd_for_rock(const client *c);
list* bzpopmax_cmd_for_rock(const client *c);
list* zrandmember_cmd_for_rock(const client *c);

// t_hash.c
list* hset_cmd_for_rock(const client *c);
list* hsetnx_cmd_for_rock(const client *c);
list* hget_cmd_for_rock(const client *c);
list* hmset_cmd_for_rock(const client *c);
list* hmget_cmd_for_rock(const client *c);
list* hincrby_cmd_for_rock(const client *c);
list* hincrbyfloat_cmd_for_rock(const client *c);
list* hdel_cmd_for_rock(const client *c);
list* hlen_cmd_for_rock(const client *c);
list* hstrlen_cmd_for_rock(const client *c);
list* hkeys_cmd_for_rock(const client *c);
list* hvals_cmd_for_rock(const client *c);
list* hgetall_cmd_for_rock(const client *c);
list* hexists_cmd_for_rock(const client *c);
list* hrandfield_cmd_for_rock(const client *c);
list* hscan_cmd_for_rock(const client *c);

// db.c
list* move_cmd_for_rock(const client *c);
list* copy_cmd_for_rock(const client *c);
list* rename_cmd_for_rock(const client *c);
list* renamenx_cmd_for_rock(const client *c);

// expire.c
list* expire_cmd_for_rock(const client *c);
list* expireat_cmd_for_rock(const client *c);
list* pexpire_cmd_for_rock(const client *c);
list* pexpireat_cmd_for_rock(const client *c);
list* ttl_cmd_for_rock(const client *c);
list* touch_cmd_for_rock(const client *c);
list* pttl_cmd_for_rock(const client *c);
list* persist_cmd_for_rock(const client *c);

// multi.c
list* exec_cmd_for_rock(const client *c);

// sort.c
list* sort_cmd_for_rock(const client *c);

// debug.c
list* debug_cmd_for_rock(const client *c);

// cluster.c
list* migrate_cmd_for_rock(const client *c);
list* dump_cmd_for_rock(const client *c);

// object.c
list* object_cmd_for_rock(const client *c);

// geo.c
list* geoadd_cmd_for_rock(const client *c);
list* georadius_cmd_for_rock(const client *c);
list* georadius_ro_cmd_for_rock(const client *c);
list* georadiusbymember_cmd_for_rock(const client *c);
list* georadiusbymember_ro_cmd_for_rock(const client *c);
list* geohash_cmd_for_rock(const client *c);
list* geopos_cmd_for_rock(const client *c);
list* geodist_cmd_for_rock(const client *c);
list* geosearch_cmd_for_rock(const client *c);
list* geosearchstore_cmd_for_rock(const client *c);

// hyperloglog.c
list* pfadd_cmd_for_rock(const client *c);
list* pfcount_cmd_for_rock(const client *c);
list* pfmerge_cmd_for_rock(const client *c);
list* pfdebug_cmd_for_rock(const client *c);

#endif
