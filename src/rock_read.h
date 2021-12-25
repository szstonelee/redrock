#ifndef __ROCK_READ_H
#define __ROCK_READ_H

#include "sds.h"
#include "adlist.h"
#include "server.h"

extern pthread_t rock_read_thread_id;

void init_and_start_rock_read_thread();

int on_client_need_rock_keys_for_db(client *c, const list *redis_keys);
int on_client_need_rock_fields_for_hashes(client *c, const list *hash_keys, const list *hash_fields);

int debug_check_no_candidates(const int len, const sds *rock_keys);

/* for read_write.c */
int already_in_candidates_for_db(const int dbid, const sds redis_key);
int already_in_candidates_for_hash(const int dbid, const sds redis_key, const sds field);

#endif