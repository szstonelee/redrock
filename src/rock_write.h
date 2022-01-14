#ifndef __ROCK_WRITE_H
#define __ROCK_WRITE_H

#include "sds.h"
#include "server.h"

// NOTE: The less the ring_buffer_len, the more time need to evicition but less chance of time out of eviction  
// #define RING_BUFFER_LEN  8       // In test, we can not evict to 100M (only 40M) for a long time
#define RING_BUFFER_LEN  16         // In test, we can use tens of minutes to evict 100M
// #define RING_BUFFER_LEN 2  // for dbug, NOTE: if setting 1, compiler will generate some warnings

extern pthread_t rock_write_thread_id;

// for server.c
void init_and_start_rock_write_thread();    

// for command ROCKEVICT and ROCKEVICTHASH
#define TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL     0
#define TRY_EVICT_ONE_SUCCESS                       1
// #define TRY_EVICT_ONE_FAIL_FOR_DUPICATE_KEY         -1
int try_evict_one_key_to_rocksdb(const int dbid, const sds key, size_t *mem);
int try_evict_one_field_to_rocksdb(const int dbid, const sds key, const sds field, size_t *mem);

// for rock_read.c
list* get_vals_from_write_ring_buf_first_for_db(const int dbid, const list *redis_keys);
list* get_vals_from_write_ring_buf_first_for_hash(const int dbid, const list *hash_keys, const list *fields);

// for rock_rdb_aof.c
sds get_key_val_str_from_write_ring_buf_first_in_redis_process(const int dbid, const sds key);
sds get_field_val_str_from_write_ring_buf_first_in_redis_process(const int dbid, const sds hash_key, const sds field);

// for rock_rdb_aof.c
void create_snapshot_of_ring_buf_for_child_process(sds *keys, sds *vals);

#endif