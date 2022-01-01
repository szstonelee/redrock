#ifndef __ROCK_HASH_H
#define __ROCK_HASH_H

#include "server.h"

dict* init_rock_hash_dict();
void init_rock_hash_before_enter_event_loop();

void on_hash_key_add_field(const int dbid, const sds redis_key, const sds field);
void on_hash_key_del_field(const int dbid, const sds redis_key, const sds field);
void on_del_key_from_db_for_rock_hash(const int dbid, const sds redis_key);
void on_overwrite_key_from_db_for_rock_hash(const int dbid, const sds redis_key, const robj *new_o);
void on_visit_field_of_hash_for_readonly(const int dbid, const sds redis_key, const sds field);
void on_visit_all_fields_of_hash_for_readonly(const int dbid, const sds redis_key);
void on_overwrite_field_for_rock_hash(const int dbid, const sds redis_key, const sds field, const int is_field_rock_value_before);
void on_rockval_field_of_hash(const int dbid, const sds redis_key, const sds field);
void on_recover_field_of_hash(const int dbid, const sds redis_key, const sds field);
void on_empty_db_for_hash(const int dbnum);

int is_in_rock_hash(const int dbid, const sds redis_key);

#endif