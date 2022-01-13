#ifndef __ROCK_RDB_AOF_H
#define __ROCK_RDB_AOF_H

#include "server.h"

void init_for_rdb_aof_service();
int on_start_rdb_aof_process();
void set_process_id_in_child_process_for_rock();
void on_exit_rdb_aof_process();
// void close_pipe_after_child_process_start_in_redis_process();
void close_unused_pipe_in_child_process_when_start();
void signal_child_process_already_running();

robj* get_value_if_exist_in_rock_for_rdb_afo(const robj *o, const int dbid, const sds key);

#endif