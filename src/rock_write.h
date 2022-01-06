#ifndef __ROCK_WRITE_H
#define __ROCK_WRITE_H

#include "sds.h"
#include "server.h"

extern pthread_t rock_write_thread_id;


void init_and_start_rock_write_thread();    // for server.c

// for server cron
// int space_in_write_ring_buffer();  

// for server cron
// int try_evict_to_rocksdb_for_db(const int try_len, const int *try_dbids, const sds *try_keys, const int from_cron); 
// int try_evict_to_rocksdb_for_hash(const int try_len, const int *try_dbids, const sds *try_keys, const sds *try_fields, const int from_cron);

// for command ROCKEVICT and ROCKEVICTHASH
#define TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL     0
#define TRY_EVICT_ONE_SUCCESS                       1
// #define TRY_EVICT_ONE_FAIL_FOR_DUPICATE_KEY         -1
int try_evict_one_key_to_rocksdb(const int dbid, const sds key, size_t *mem);
int try_evict_one_field_to_rocksdb(const int dbid, const sds key, const sds field, size_t *mem);

// for rock_read.c
list* get_vals_from_write_ring_buf_first_for_db(const int dbid, const list *redis_keys);
list* get_vals_from_write_ring_buf_first_for_hash(const int dbid, const list *hash_keys, const list *fields);

// for flushdb or flushall commands
// void on_empty_db_for_rock_write(const int dbnum);

#endif