#ifndef __ROCK_RDB_AOF_H
#define __ROCK_RDB_AOF_H

#include "server.h"

int on_start_rdb_aof_process();
void set_process_id_in_child_process_for_rock();
void on_exit_rdb_aof_process();

robj* get_value_if_exist_in_rock_for_rdb_afo(const robj *o, const int dbid, const sds key);

#endif