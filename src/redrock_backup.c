/* Here I save some code which is not used but maybe referennce
 */

/* rock_reead.c */

static inline int is_rock_key_has_dbid(const int dbid, const sds rock_key)
{
    const int dbid_in_rock_key = rock_key[0];
    return dbid == dbid_in_rock_key ? 1 : 0;
}

static inline int is_rock_key_has_dbid(const int dbid, const sds rock_key)
{
    const int dbid_in_rock_key = rock_key[0];
    return dbid == dbid_in_rock_key ? 1 : 0;
}


/* Called in main thread when delete a whole db.
 * Reference delete_key() but this implementation is quicker
 */
static void delete_whole_db(const int dbid, list *client_ids)
{
    rock_r_lock();
    // first deal with read_key_tasks
    for (int i = 0; i < READ_TOTAL_LEN; ++i)
    {
        sds task = read_key_tasks[i];
        if (task == NULL)
            break;

        if (task == outdate_key_flag)
            continue;

        if (is_rock_key_has_dbid(dbid, task))
        {
            read_key_tasks[i] = outdate_key_flag;
            if (read_return_vals[i] != NULL)
                sdsfree(read_return_vals[i]);
        }        
    }

    // second deal with read_rock_key_candidates
    list *will_deleted = listCreate();
    dictIterator *di = dictGetIterator(read_rock_key_candidates);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        sds rock_key = dictGetKey(de);
        if (is_rock_key_has_dbid(dbid, rock_key))
        {
            listAddNodeTail(will_deleted, rock_key);
            list *waiting = dictGetVal(de);
            dictGetVal(de) = NULL;
            listJoin(client_ids, waiting);
            serverAssert(listLength(waiting) == 0);
            listRelease(waiting);
        }
    }
    dictReleaseIterator(di);
    // delete will_deleted from read_rock_key_candidates
    listIter li;
    listNode *ln;
    listRewind(will_deleted, &li);
    while ((ln = listNext(&li)))
    {
        sds delete_rock_key = listNodeValue(ln);
        int res = dictDelete(read_rock_key_candidates, delete_rock_key);
        serverAssert(res == DICT_OK);
    }
    listRelease(will_deleted);
    rock_r_unlock();
}

/* Called in main thread
 * The caller guarantee that all keys in db of dbid must be deleted first.
 */
void on_delete_whole_db_in_command(const int dbid)
{
    list *client_ids = listCreate();
    delete_whole_db(dbid, client_ids);
    clients_check_resume_for_rock_key_update(client_ids);
    listRelease(client_ids);
}

/* Called in main thread.
 * The caller guarantee that the keys must be deleted from redis db first
 * then call into the on_delete_keys_in_command().
 * Such command like DEL, visit expired key, MIGRATE, MOVE, UNLINK
 * So the resume commands can guarantee the atomic in order.
 */
void on_delete_keys_in_command(const int dbid, const list* redis_keys)
{
    list *client_ids = listCreate();
    listIter li;
    listNode *ln;
    listRewind((list*)redis_keys, &li);
    while ((ln = listNext(&li)))
    {
        const sds redis_key = listNodeValue(ln);
        delete_key(dbid, redis_key, client_ids);
    }
    clients_check_resume_for_rock_key_update(client_ids);
    listRelease(client_ids);
}

void debug_add_tasks(const int cnt, const int* const dbids, const sds* keys)
{
    rock_r_lock();

    for (int i = 0; i < cnt; ++i)
    {
        sds copy = sdsdup(keys[i]);
        copy = encode_rock_key(dbids[i], copy);

        read_key_tasks[i] = copy;
        read_return_vals[i] = NULL;
    }

    task_status = READ_START_TASK;

    rock_r_unlock();
}

/* This is called in main thread and as the API for rock_write.c
 * when it will write some rock keys to RocksDB
 * We will delete all rock_keys in read_rock_key_candidates
 * with setting outdate_key_flag in read_key_tasks 
 * before they are writtent to RocksDB.
 */
void cancel_read_task_before_write_to_rocksdb(const int len, const sds *rock_keys)
{
    rock_r_lock();

    for (int i = 0; i < len; ++i)
    {
        dictEntry *de = dictFind(read_rock_key_candidates, rock_keys[i]);
        if (de)
        {
            const sds rock_key_in_candidates = dictGetKey(de);
            set_outdate_flag(rock_key_in_candidates);
            // After set outdate flag, we can delete the candidate.
            // We do not need to resume the clients because 
            // the API caller guarantee these keys change to rock val.
            int res = dictDelete(read_rock_key_candidates, rock_key_in_candidates);
            serverAssert(res == DICT_OK);
        }
    }

    rock_r_unlock();
}

/* This is called in main thread to set outdate flag
 * It guarantee in lock mode by the caller.
 * with freeing return val if needed.
 */
static void set_outdate_flag(const sds rock_key_in_candidates)
{
    for (int i = 0; i < READ_TOTAL_LEN; ++i)
    {
        sds task = read_key_tasks[i];
        if (task == NULL)
            break;

        // Because read_key_tasks share the same key with candidates
        // We can use address comparison
        if (task == rock_key_in_candidates)
        {
            read_key_tasks[i] = outdate_key_flag;
            if (read_return_vals[i])
                sdsfree(read_return_vals[i]);
            break;
        }
    }
}

/* Work in read thead to read the needed real keys, i.e., rocksdb_keys.
 * The caller guarantees not in lock mode.
 * NOTE: no need to work in lock mode because keys is duplicated from read_key_tasks
 */
static int read_from_rocksdb(const int cnt, const sds* keys, sds* vals)
{
    char* rockdb_keys[READ_TOTAL_LEN];
    size_t rockdb_key_sizes[READ_TOTAL_LEN];
    char* rockdb_vals[READ_TOTAL_LEN];
    size_t rockdb_val_sizes[READ_TOTAL_LEN];
    char* errs[READ_TOTAL_LEN];

    int real_read_cnt = 0;
    for (int i = 0; i < cnt; ++i)
    {
        serverAssert(keys[i]);
        if (keys[i] == outdate_key_flag)
            continue;
        
        rockdb_keys[real_read_cnt] = keys[i];
        rockdb_key_sizes[real_read_cnt] = sdslen(keys[i]);
        ++real_read_cnt;
    }

    if (real_read_cnt == 0)
        return 0;   // all input keys are outdate_key_flag

    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    rocksdb_multi_get(rockdb, readoptions, real_read_cnt, 
                      (const char* const *)rockdb_keys, rockdb_key_sizes, 
                      rockdb_vals, rockdb_val_sizes, errs);
    rocksdb_readoptions_destroy(readoptions);

    int index_input = 0;
    int index_rock = 0;
    while (index_input < cnt)
    {
        if (keys[index_input] == outdate_key_flag)
        {
            ++index_input;
            continue;       // skip outdate_key_flag
        }
        
        if (errs[index_rock]) 
        {
            serverLog(LL_WARNING, "read_from_rocksdb() reading from RocksDB failed, err = %s, key = %s",
                      errs[index_rock], rockdb_keys[index_rock]);
            exit(1);
        }

        if (rockdb_vals[index_rock] == NULL)
        {
            // not found in RocksDB
            vals[index_input] = NULL;
        }
        else
        {
            // I think this code can not reroder in the caller's lock scope 
            vals[index_input] = sdsnewlen(rockdb_vals[index_rock], rockdb_val_sizes[index_rock]);
            // free the malloc memory from RocksDB API
            zlibc_free(rockdb_vals[index_rock]);  
        }

        ++index_input;
        ++index_rock;
    }    

    return real_read_cnt;
}

/* This is called by proocessInputBuffer() in netwroking.c
 * It will check the rock keys for current command in buffer
 * and if OK process the command and return.
 * Return C_ERR if processCommandAndResetClient() return C_ERR
 * indicating the caller need return to avoid looping and trimming the client buffer.
 * Otherwise, return C_OK, indicating in the caller, it can continue in the loop.
 */
int processCommandAndResetClient(client *c);        // networkng.c, no declaration in any header
int process_cmd_in_processInputBuffer(client *c)
{
    int ret = C_OK;

    list *rock_keys = get_keys_in_rock_for_command(c);
    if (rock_keys == NULL)
    {
        // NO rock_key or TRANSACTION with no EXEC command
        if (processCommandAndResetClient(c) == C_ERR)
            ret = C_ERR;
    }
    else
    {
        const int sync_mode = on_client_need_rock_keys(c, rock_keys);
        if (sync_mode)
        {
            if (processCommandAndResetClient(c) == C_ERR)
                ret = C_ERR;
        }
        listRelease(rock_keys);
    }

    return ret;
}

/* Called in main thread to recover one key, i.e., rock_key.
 * The caller guarantees lock mode, 
 * so be careful of no reentry of the lock.
 * Join (by moving to append) the waiting list for curent key to waiting_clients,
 * and delete the key from read_rock_key_candidates 
 * without destroy the waiting list for current rock_key.
 */
static void recover_one_key(const sds rock_key, const sds recover_val,
                            list *waiting_clients)
{
    int dbid;
    char *redis_key;
    size_t redis_key_len;
    decode_rock_key(rock_key, &dbid, &redis_key, &redis_key_len);
    try_recover_val_object_in_redis_db(dbid, redis_key, redis_key_len, recover_val);

    dictEntry *de = dictFind(read_rock_key_candidates, rock_key);
    serverAssert(de);

    list *current = dictGetVal(de);
    serverAssert(current);

    dictGetVal(de) = NULL;      // avoid clear the list of client ids in read_rock_key_candidates
    // task resource will be reclaimed (the list is NULL right now)
    dictDelete(read_rock_key_candidates, rock_key);

    listJoin(waiting_clients, current);
    serverAssert(listLength(current) == 0);
    listRelease(current);
}
