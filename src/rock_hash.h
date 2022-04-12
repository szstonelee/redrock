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

#ifndef __ROCK_HASH_H
#define __ROCK_HASH_H

#include "server.h"

dict* init_rock_hash_dict();
void init_rock_hash_before_enter_event_loop();

void on_hash_key_add_field(const int dbid, const sds redis_key, const sds field);
void on_hash_key_del_field(const int dbid, const sds redis_key, const sds field);
void on_del_key_from_db_for_rock_hash(const int dbid, const sds redis_key);
void on_overwrite_key_from_db_for_rock_hash(const int dbid, const sds redis_key, const size_t old_field_cnt, const robj *new_o);
void on_visit_field_of_hash_for_readonly(const int dbid, const sds redis_key, const sds field);
void on_visit_all_fields_of_hash_for_readonly(const int dbid, const sds redis_key);
void on_overwrite_field_for_rock_hash(const int dbid, const sds redis_key, const sds field, const int is_field_rock_value_before);
void on_rockval_field_of_hash(const int dbid, const sds redis_key, const sds field);
void on_recover_field_of_hash(const int dbid, const sds redis_key, const sds field);
void on_empty_db_for_hash(const int dbnum);

int is_in_rock_hash(const int dbid, const sds redis_key);

dict* create_empty_lrus_for_rock_hash();

#endif
