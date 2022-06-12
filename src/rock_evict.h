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

#ifndef __ROCK_EVICT_H
#define __ROCK_EVICT_H

#include "server.h"
#include "rock.h"

#ifdef RED_ROCK_EVICT_INFO
void eviction_info_print();
#endif

dict* init_rock_evict_dict(const int dbid);
void init_rock_evict_before_enter_event_loop();

void add_whole_redis_hash_to_rock_hash(const int dbid, const sds redis_key, const int rdb_loading);

void on_db_add_key_for_rock_evict_or_rock_hash(const int dbid, const sds internal_key);
void on_db_del_key_for_rock_evict(const int dbid, const sds key);
void on_db_overwrite_key_for_rock_evict(const int dbid, const sds key, const int is_old_rock_val, const robj *new_o);
void on_transfer_to_rock_hash(const int dbid, const sds internal_key);
void on_db_visit_key_for_rock_evict(const int dbid, const sds key);
void on_rockval_key_for_rock_evict(const int dbid, const sds internal_key);
void on_recover_key_for_rock_evict(const int dbid, const sds internal_key);
void on_empty_db_for_rock_evict(const int dbnum);

void evict_pool_init();

// for test
// size_t perform_key_eviction(const size_t want_to_free);
// size_t perform_field_eviction(const size_t want_to_free);
int perform_rock_eviction_in_cron();
size_t perform_rock_eviction_for_rock_mem(const size_t want_to_free, const size_t timeout_in_ms);

#endif
