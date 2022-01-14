#ifndef __ROCK_RDB_AOF_H
#define __ROCK_RDB_AOF_H

#include "server.h"

// for redis process
void init_for_rdb_aof_service();

int on_start_rdb_aof_process();
void signal_child_process_already_running(const int child_pid);

// for child process
void on_start_in_child_process();
void on_exit_rdb_aof_process();

// for whole situatioons: child process and not child process
robj* get_value_if_exist_in_rock_for_rdb_afo(const robj *o, const int dbid, const sds key);

#endif