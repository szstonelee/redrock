#ifndef __ROCK_WRITE_H
#define __ROCK_WRITE_H

#include "sds.h"
#include "server.h"

// also for server cron
#define RING_BUFFER_LEN 64

void init_and_start_rock_write_thread();    // for server.c

// for server cron
int space_in_write_ring_buffer();  

// for server cron
// void write_batch_append_and_abandon(const int len, const int* dbids, sds* keys, robj** objs);  
int try_evict_to_rocksdb(const int check_len, const int *check_dbids, const sds *check_keys); 

#endif