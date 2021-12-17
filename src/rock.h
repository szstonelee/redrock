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

// list* get_keys_in_rock_for_command(const client *c);

int process_cmd_in_processInputBuffer(client *c);


/* Check whether o is a rock value.
 * Return 1 if it is. Otherwise return 0.
 */
inline int is_rock_value(const robj *o)
{
    return  o == shared.rock_val_str_other ||
            o == shared.rock_val_str_int;
}

/* Check whether o is a shared value which is made by makeObjectShared()
 * Return 1 if true. Otherwise return 0.
 */
inline int is_shared_value(const robj *o)
{
    return o->refcount == OBJ_SHARED_REFCOUNT;
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
list* generic_get_zset_num_for_rock(const client *c);

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

// bitop.c
list* setbit_cmd_for_rock(const client *c);
list* getbit_cmd_for_rock(const client* c);
list* bitfield_cmd_for_rock(const client* c);
list* bitfield_ro_cmd_for_rock(const client *c);

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

#endif
