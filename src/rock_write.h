/* RedRock is based on Redis, coded by Tony. The copyright is same as Redis.
 *
 * Copyright (c) 2018, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __ROCK_WRITE_H
#define __ROCK_WRITE_H

#include "sds.h"
#include "server.h"

// NOTE: The less the ring_buffer_len, the more time need to evicition but less chance of time out of eviction  
// #define RING_BUFFER_LEN  8       // In test, we can not evict to 100M (only 40M) for a long time
#define RING_BUFFER_LEN  16         // In test, we can use tens of minutes to evict 100M
// #define RING_BUFFER_LEN 2  // for dbug, NOTE: if setting 1, compiler will generate some warnings

// extern pthread_t rock_write_thread_id;
void join_write_thread();

// for server.c
void init_and_start_rock_write_thread();    

// for command ROCKEVICT and ROCKEVICTHASH
#define TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL     0
#define TRY_EVICT_ONE_SUCCESS                       1
// #define TRY_EVICT_ONE_FAIL_FOR_DUPICATE_KEY         -1
int try_evict_one_key_to_rocksdb(const int dbid, const sds key, size_t *mem);
int try_evict_one_field_to_rocksdb(const int dbid, const sds key, const sds field, size_t *mem);

// for main thread when loading
void write_to_rocksdb_in_main_for_key_when_load(redisDb *db, const sds redis_key, const robj *redis_val);
void write_to_rocksdb_in_main_for_hash_when_load(redisDb *db, const sds redis_key, const sds field, const sds field_val);

// for rock_read.c
list* get_vals_from_write_ring_buf_first_for_db(const int dbid, const list *redis_keys);
list* get_vals_from_write_ring_buf_first_for_hash(const int dbid, const list *hash_keys, const list *fields);

// for rock_rdb_aof.c
sds get_key_val_str_from_write_ring_buf_first_in_redis_process(const int dbid, const sds key);
sds get_field_val_str_from_write_ring_buf_first_in_redis_process(const int dbid, const sds hash_key, const sds field);

// for rock_rdb_aof.c
void create_snapshot_of_ring_buf_for_child_process(sds *keys, sds *vals);

// for rock.c and rock_purge.c (no lock)
void rock_w_signal_cond();
// for rock_purge.c (with lock)
void try_to_wakeup_write_thread();

// for main thread purge job
int is_eviction_ring_buffer_empty();
int has_unfinished_purge_task_for_write();
void transfer_purge_task_to_write_thread(int db_cnt, int *db_dbids, sds *db_keys,
                                         int hash_cnt, int *hash_dbids, sds *hash_keys, sds *hash_fields);
                                
#endif
