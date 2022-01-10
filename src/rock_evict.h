#ifndef __ROCK_EVICT_H
#define __ROCK_EVICT_H

#include "server.h"
#include "rock.h"

#ifdef RED_ROCK_EVICT_INFO
void eviction_info_print();
#endif

dict* init_rock_evict_dict();
void init_rock_evict_before_enter_event_loop();

void on_db_add_key_for_rock_evict(const int dbid, const sds internal_key);
void on_db_del_key_for_rock_evict(const int dbid, const sds key);
void on_db_overwrite_key_for_rock_evict(const int dbid, const sds key, const robj *new_o);
void on_transfer_to_rock_hash(const int dbid, const sds internal_key);
void on_db_visit_key_for_rock_evict(const int dbid, const sds key);
void on_rockval_key_for_rock_evict(const int dbid, const sds internal_key);
void on_recover_key_for_rock_evict(const int dbid, const sds internal_key);
void on_empty_db_for_rock_evict(const int dbnum);

void evict_pool_init();

// for test
// size_t perform_key_eviction(const size_t want_to_free);
// size_t perform_field_eviction(const size_t want_to_free);
void perform_rock_eviction_in_cron();
size_t perform_rock_eviction_for_rock_mem(const size_t want_to_free, const size_t timeout_in_ms);

#endif