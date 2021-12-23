#ifndef __ROCK_HASH_H
#define __ROCK_HASH_H

#include "server.h"

dict* init_rock_hash_dict();
void on_hash_key_add_field(const int dbid, const sds redis_key, const robj *o, const sds field);
void on_hash_key_del_field(const int dbid, const sds redis_key, const robj *o, const sds field);
void on_del_hash_from_db(const int dbid, const sds redis_key);
void on_visit_field_of_hash(const int dbid, const sds redis_key, const robj *o, const sds field);
void on_recover_field_of_hash(const int dbid, const sds redis_key, const robj *o, const sds field);

#endif