#ifndef __ROCK_WRITE_H
#define __ROCK_WRITE_H

#include "sds.h"
#include "server.h"

extern pthread_t rock_write_thread_id;

// also for server cron
#define RING_BUFFER_LEN 64

void init_and_start_rock_write_thread();    // for server.c

// for server cron
int space_in_write_ring_buffer();  

// for server cron
int try_evict_to_rocksdb_for_db(const int try_len, const int *try_dbids, const sds *try_keys); 
int try_evict_to_rocksdb_for_hash(const int try_len, const int *try_dbids, const sds *try_keys, const sds *try_fields);

// for command ROCKEVICT
int try_evict_one_key_to_rocksdb(const int dbid, const sds key);

// for rock_read.c
list* get_vals_from_write_ring_buf_first(const int dbid, const list *redis_keys);

#endif