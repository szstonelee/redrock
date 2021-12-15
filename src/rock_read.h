#ifndef __ROCK_READ_H
#define __ROCK_READ_H

#include "sds.h"
#include "adlist.h"
#include "server.h"

void init_and_start_rock_read_thread();   // for server.c

void on_client_need_rock_keys(client *c, const list *redis_keys);
int debug_check_no_candidates(const int len, const sds *rock_keys);
int already_in_candidates(const int dbid, const sds redis_key);

#endif