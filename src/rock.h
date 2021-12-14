#ifndef __ROCK_H
#define __ROCK_H

#include "server.h"
#include <rocksdb/c.h>

#define RED_ROCK_DEBUG      // run debug code if defined this macro. In release build, comment this line

// the global rocksdb handler
extern rocksdb_t* rockdb;   

void create_shared_object_for_rock();
void init_rocksdb(const char* folder_path);
void debug_rock(client *c);
sds encode_rock_key(const int dbid, sds redis_to_rock_key);
void decode_rock_key(const sds rock_key, int* dbid, char** redis_key, size_t* key_sz);

void init_client_id_table();
client* lookup_client_from_id(const uint64_t client_id);
void on_add_a_new_client(client* const c);
void on_del_a_destroy_client(const client* const c);

/* Check whether o is a rock value.
 * Return 1 if it is. Otherwise return 0.
 */
inline int is_rock_value(const robj *o)
{
    if (o == shared.rock_val_str_int)
    {
        return 1;
    }
    else if (o == shared.rock_val_str_other)
    {
        return 1;
    } 
    else
    {
        return 0;
    }
}

/* Check whether o is a shared value which is made by makeObjectShared()
 * Return 1 if true. Otherwise return 0.
 */
inline int is_shared_value(const robj *o)
{
    return o->refcount == OBJ_SHARED_REFCOUNT ? 1 : 0;
}

#endif
