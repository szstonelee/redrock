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


static void debug_check_sds_equal(const int dbid, const sds redis_key, const robj *o, const sds field)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, redis_key);
    serverAssert(de_db);
    serverAssert(dictGetKey(de_db) == redis_key);
    serverAssert(dictGetVal(de_db) == o);

    dict *hash = o->ptr;
    dictEntry *de_hash = dictFind(hash, field);
    serverAssert(de_hash);
    serverAssert(dictGetKey(de_hash) == field);
}

void debug_rock(client *c)
{
    sds flag = c->argv[1]->ptr;

    if (strcasecmp(flag, "evictkeys") == 0 && c->argc >= 3)
    {
        sds keys[RING_BUFFER_LEN];
        int dbids[RING_BUFFER_LEN];
        int len = 0;
        for (int i = 0; i < c->argc-2; ++i)
        {
            sds input_key = c->argv[i+2]->ptr;
            dictEntry *de = dictFind(c->db->dict, input_key);
            if (de)
            {
                if (!is_rock_value(dictGetVal(de)))
                {
                    serverLog(LL_NOTICE, "debug evictkeys, try key = %s", input_key);
                    keys[len] = input_key;
                    dbids[len] = c->db->id;
                    ++len;
                }
            }
        }
        if (len)
        {
            int ecvict_num = try_evict_to_rocksdb_for_db(len, dbids, keys);
            serverLog(LL_NOTICE, "debug evictkeys, ecvict_num = %d", ecvict_num);
        }
    }
    else if (strcasecmp(flag, "recoverkeys") == 0 && c->argc >= 3)
    {
        serverAssert(c->rock_key_num == 0);
        list *rock_keys = listCreate();
        for (int i = 0; i < c->argc-2; ++i)
        {
            sds input_key = c->argv[i+2]->ptr;
            dictEntry *de = dictFind(c->db->dict, input_key);
            if (de)
            {
                if (is_rock_value(dictGetVal(de)))
                {
                    serverLog(LL_WARNING, "debug_rock() found rock key = %s", (sds)dictGetKey(de));
                    listAddNodeTail(rock_keys, dictGetKey(de));
                }
            }
        }
        if (listLength(rock_keys) != 0)
        {
            on_client_need_rock_keys_for_db(c, rock_keys);
        }
        listRelease(rock_keys);
    }
    else if (strcasecmp(flag, "testread") == 0)
    {
        /*
        sds keys[2];
        keys[0] = sdsnew("key1");
        keys[1] = sdsnew("key2");
        sds copy_keys[2];
        copy_keys[0] = sdsdup(keys[0]);
        copy_keys[1] = sdsdup(keys[1]);
        robj* objs[2];
        char* val1 = "val_for_key1";
        char* val2 = "val_for_key2";       
        objs[0] = createStringObject(val1, strlen(val1));
        objs[1] = createStringObject(val2, strlen(val2));
        int dbids[2];
        dbids[0] = 65;      // like letter 'a'
        dbids[1] = 66;      // like letter 'b'
        write_batch_append_and_abandon(2, dbids, keys, objs);
        sleep(1);       // waiting for save to RocksdB
        debug_add_tasks(2, dbids, copy_keys);
        on_delete_key(dbids[0], copy_keys[1]);
        */
    }
    else if (strcasecmp(flag, "testwrite") == 0) 
    {
        /*
        int dbid = 1;

        int val_len = random() % 1024;
        sds val = sdsempty();
        for (int i = 0; i < val_len; ++i)
        {
            val = sdscat(val, "v");
        }

        sds keys[RING_BUFFER_LEN];
        robj* objs[RING_BUFFER_LEN];
        int dbids[RING_BUFFER_LEN];
        int cnt = 0;
        while (cnt < 5000000)
        {
            int space = space_in_write_ring_buffer();
            if (space == 0)
            {
                serverLog(LL_NOTICE, "space = 0, sleep for a while, cnt = %d", cnt);
                usleep(10000);
                continue;
            }
            
            int random_pick = random() % RING_BUFFER_LEN;
            if (random_pick == 0)
                random_pick = 1;
            
            const int pick = random_pick < space ? random_pick : space;

            for (int i = 0; i < pick; ++i)
            {
                sds key = debug_random_sds(128);
                sds val = debug_random_sds(1024);

                keys[i] = key;
                robj* o = createStringObject(val, sdslen(val));
                sdsfree(val);
                objs[i] = o;
                dbids[i] = dbid;
            }         
            write_batch_append_and_abandon(pick, dbids, keys, objs); 
            cnt += pick;
            serverLog(LL_NOTICE, "write_batch_append total = %d, cnt = %d", space, cnt);
        }
        */
    }
    else
    {
        addReplyError(c, "wrong flag for debugrock!");
        return;
    }

    addReplyBulk(c,c->argv[0]);
}




static void debug_print_lrus(const dict *lrus)
{
    dictIterator *di = dictGetIterator((dict*)lrus);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        const sds field = dictGetKey(de);
        serverLog(LL_NOTICE, "debug_print_lrus, field = %s", field);
    }

    dictReleaseIterator(di);
}

/* Called in main thread to set invalid flag for ring buffer.
 * The caller guarantee in lock mode.
 */
static void invalid_ring_buf_for_dbid(const int dbid)
{
    int index = rbuf_s_index;
    for (int i = 0; i < rbuf_len; ++i)
    {
        const sds rock_key = rbuf_keys[index];
        const int dbid_in_key = (unsigned char)rock_key[1];
        /*
        if (dbid_in_key == dbid)
            rbuf_invalids[index] = 1;
        */

        ++index;
        if (index == RING_BUFFER_LEN)
            index = 0;
    }
}

/* Called in main thread for flushdb or flushall( dbnum == -1) command
 * We need to set rbuf_invalids to true for these dbs,
 * because ring buffer can not removed from the middle,
 * but the rock read will lookup them from ring buufer by API 
 * get_vals_from_write_ring_buf_first_for_db() and get_vals_from_write_ring_buf_first_for_hash()
 */
void on_empty_db_for_rock_write(const int dbnum)
{
    return;     // do nothing

    int start = dbnum;
    if (dbnum == -1)
        start = 0;

    int end = dbnum + 1;
    if (dbnum == -1)
        end = server.dbnum;

    rock_w_lock();

    if (rbuf_len == 0)
    {
        rock_w_unlock();
        return;
    }

    for (int dbid = start; dbid < end; ++dbid)
    {
        invalid_ring_buf_for_dbid(dbid);
    }

    rock_w_unlock();
}

static void debug_mem()
{
    int cnt = 0;
    // void *p = (void*)&server.hz;

    while (1)
    {
        sds field = sdsnew("f1");
        sds value = sdsnew("v1");

        int update = 0;
        robj *o = createHashObject();
        unsigned char *zl, *fptr, *vptr;
        zl = o->ptr;

        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = ziplistNext(zl, fptr);
                serverAssert(vptr != NULL);
                update = 1;

                /* Replace value */
                zl = ziplistReplace(zl, vptr, (unsigned char*)value,
                        sdslen(value));
            }
        }
        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            zl = ziplistPush(zl, (unsigned char*)field, sdslen(field),
                    ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)value, sdslen(value),
                    ZIPLIST_TAIL);
        }
        o->ptr = zl;

        sds v = marshal_object(o);
        decrRefCount(o);
        robj *r = unmarshal_object(v);
        sdsfree(v);
        decrRefCount(r);

        sdsfree(field);
        sdsfree(value);

        ++cnt;
        if (cnt == 10000000)
        {
            serverLog(LL_WARNING, "used mem = %lu", zmalloc_used_memory());
            cnt = 0;
        }
    }
}


/* Called in main thread when evict some keys to RocksDB in rock_write.c.
 * The caller guarantee not use read lock.
 * If the rock_keys are not in candidatte, return 1 meaning check pass.
 * Otherwise, return 0 meaning there is a bug,
 * because we can not evict to RocksDB when the rock_key is in candidates.
 * The logic must guaratee that the candidate must be processed then we can evict.
 */
int debug_check_no_candidates(const int len, const sds *rock_keys)
{
    int no_exist = 1;

    rock_r_lock();
    for (int i = 0; i < len; ++i)
    {
        if (dictFind(read_rock_key_candidates, rock_keys[i]))
        {            
            no_exist = 0;
            break;
        }
    }
    rock_r_unlock();

    return no_exist;
}

/* For debug command, i.e. debugrock ... */
void debug_rock(client *c)
{
    sds flag = c->argv[1]->ptr;

    if (strcasecmp(flag, "mem") ==  0)
    {
        // debug_mem();
        debug_vc();
    }
    else if (strcasecmp(flag, "evictkeys") == 0 && c->argc >= 3)
    {
    }
    else if (strcasecmp(flag, "recoverkeys") == 0 && c->argc >= 3)
    {
    }
    else if (strcasecmp(flag, "testwrite") == 0) 
    {
    }
    else
    {
        addReplyError(c, "wrong flag for debugrock!");
        return;
    }

    addReplyBulk(c,c->argv[0]);
}

static void debug_vc()
{
    client *vc = createClient(NULL);
    on_add_a_new_client(vc);

    const char *key_name = "abc";
    const char *field1_name = "f1";
    const char *field2_name = "f2";
    const char *val1_name = "val1";
    const char *val2_name = "val2";
    const int dbid = 0;
    redisDb *db = server.db + dbid;
    
    list *redis_keys = listCreate();
    sds need_key = sdsnew(key_name);
    // sds not_exist_kkey = sdsnew("not_exist_key");
    serverAssert(listAddNodeTail(redis_keys, need_key) != NULL);
    // serverAssert(listAddNodeTail(redis_keys, not_exist_kkey) != NULL);

    int cnt = 0;
    int sync_cnt = 0;
    while (1)
    {
        robj *key = createStringObject(key_name, strlen(key_name));
        robj *val1 = createStringObject(val1_name, strlen(val1_name));
        robj *val2 = createStringObject(val2_name, strlen(val2_name));
        sds field1 = sdsnew(field1_name);
        sds field2 = sdsnew(field2_name);

        robj *o = hashTypeLookupWriteOrCreate(vc, key);
        serverAssert(o != NULL);
        serverAssert(hashTypeSet(dbid, key->ptr, o, field1, val1->ptr, HASH_SET_COPY) == 0);
        serverAssert(hashTypeSet(dbid, key->ptr, o, field2, val2->ptr, HASH_SET_COPY) == 0);        

        while (1)
        {
            int ret = try_evict_one_key_to_rocksdb_by_rockevict_command(0, key->ptr);
            if (ret == TRY_EVICT_ONE_SUCCESS)
                break;
            serverAssert(ret == TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL);
        }

        usleep(1);
        list *left = check_ring_buf_first_and_recover_for_db(dbid, redis_keys);
        if (left && listLength(left) == 0)
            ++sync_cnt;
        if (left)
            listRelease(left);    
       
#if 0
        list *vals = get_vals_from_write_ring_buf_first_for_db(dbid, redis_keys);
        if (vals)
        {
            serverAssert(listLength(vals) == listLength(redis_keys));

            list *left = listCreate();
  
            listIter li_vals;
            listNode *ln_vals;
            listIter li_keys;
            listNode *ln_keys;
            listRewind(vals, &li_vals);
            listRewind((list*)redis_keys, &li_keys);

            while ((ln_vals = listNext(&li_vals)))
            {
                ln_keys = listNext(&li_keys);

                const sds redis_key = listNodeValue(ln_keys);

                const sds recover_val = listNodeValue(ln_vals);
                if (recover_val == NULL)
                {
                    // not found in ring buffer
                    listAddNodeTail(left, redis_key);
                }
                else
                {
                    // need to recover data which is from ring buffer
                    // NOTE: no need to deal with rock_key_num in client. The caller will take care
                    dictEntry *de = dictFind(db->dict, redis_key);
                    // the redis_key must be here because it is from
                    // the caller on_client_need_rock_keys_for_db() which guaratee this
                    serverAssert(de);  

                    if (is_rock_value(dictGetVal(de)))      
                        // NOTE: the same key could repeat in redis_keys
                        //       so the second duplicated key, we can not guaratee it is rock value
                        dictGetVal(de) = unmarshal_object(recover_val);     // revocer in redis db
                }
            }
            
            listSetFreeMethod(vals, (void (*)(void*))sdsfree);
            listRelease(vals);

            listRelease(left);
        }
#endif

        serverAssert(dbDelete(db, key) == 1);

        decrRefCount(key);
        decrRefCount(val1);
        decrRefCount(val2);        
        sdsfree(field1);
        sdsfree(field2);       

        ++cnt;
        if (cnt == 10000)
        {
            serverLog(LL_WARNING, "used mem = %lu, sync_cnt = %d", zmalloc_used_memory(), sync_cnt);
            cnt = 0;
            sync_cnt = 0;
        } 
    }
}

/* Check whether the value can be evicted. 
 * Return 1 if can, otherwise, return 0.
 *
 * We exclude such cases:
 * 1. already rock value
 * 2. already shared value
 * 3. value type not suppoorted, right now, it is OBJ_STREAM  
 */
inline int is_evict_value(const robj *v)
{
    if (is_rock_value(v))
    {
        return 0;
    }
    else if (is_shared_value(v))
    {
        return 0;
    }
    else if (v->type == OBJ_STREAM)
    {
        return 0;
    }
    else
    {
        serverAssert(v->type != OBJ_MODULE);
        return 1;
    }
}



#define MAX_ESTIMATE_STEPS     32

/* We use approximate algorithm to estimate a quicklist memory
 * If quicklist count is less than or equal to MAX_ESTIMATE_STEPS,
 * it will be the acutal memory size.
 * Otherwise, we use avarge prediction for the whole quicklist.
 */
static size_t estimate_quicklist(const robj *o)
{
    quicklist *ql = o->ptr;
    size_t mem = sizeof(*ql);
    
    const size_t ql_count = quicklistCount(ql);
    if (ql_count == 0)
        return mem;

    quicklistIter *qit = quicklistGetIterator(ql, AL_START_HEAD);
    quicklistEntry entry;
    size_t steps = 0;
    size_t step_nodes_mem = 0;    
    while(quicklistNext(qit, &entry)) 
    {
        // each entry (node) encoded as ziplist
        unsigned char *zl = entry.node->zl;
        step_nodes_mem += ziplistBlobLen(zl);

        ++steps;
        if (steps == MAX_ESTIMATE_STEPS)
            break;
    }
    quicklistReleaseIterator(qit);    
    
    serverAssert(steps > 0 && ql_count >= steps);
    size_t estimate_nodes_mem = step_nodes_mem;
    if (ql_count > steps)
         estimate_nodes_mem *= ql_count / steps;
    
    mem += estimate_nodes_mem;
    mem += sizeof(quicklistNode) * ql_count;

    return mem;
}

/* We use approximate algorithm to estimate a hash memory
 * If hash count is less than or equal to MAX_ESTIMATE_STEPS,
 * it will be the acutal memory size.
 * Otherwise, we use avarge prediction for the whole hash.
 * 
 * If value_as_sds is true (i.e., 1), the value of sds is calculated as a sds pointer.
 * Otherwise, it is either a NULL pointer(like set type) or real pointer(list zset) 
 *            which we do not calculate for the memory. 
 */
static size_t estimate_hash(dict *d, const int value_as_sds)
{
    size_t mem = sizeof(dict);
    mem += sizeof(dictEntry) * (d->ht[0].size + d->ht[1].size);

    const size_t dict_cnt = dictSize(d);
    if (dict_cnt == 0)
        return mem;

    dictIterator *di = dictGetIterator(d);
    dictEntry* de;
    size_t steps = 0;
    size_t step_nodes_mem = 0;    
    while ((de = dictNext(di)))
    {
        const sds key = dictGetKey(de);
        const sds val = dictGetVal(de);

        step_nodes_mem += sdsAllocSize(key);

        if (value_as_sds)
            step_nodes_mem += sdsAllocSize(val);

        ++steps;
        if (steps == MAX_ESTIMATE_STEPS)
            break;
    }
    dictReleaseIterator(di);    

    serverAssert(steps > 0 && dict_cnt >= steps);
    size_t estimate_nodes_mem = step_nodes_mem;
    if (dict_cnt > steps)
         estimate_nodes_mem *= dict_cnt / steps;
    
    mem += estimate_nodes_mem;

    return mem;

}

staic size_t estimate_zskiplist(zskiplist *zsl)
{
    size_t mem = sizeof(zskiplist);

    const size_t zsl_cnt = zsl->length;
    if (zsl_cnt == 0)
        return mem;

    mem += zsl_cnt * sizeof(zskiplistNode);

    

    return mem;
}

static size_t estimate_mem_for_object(const robj *o)
{
    serverAssert(o);
    serverAssert(!is_rock_value(o));

    size_t mem = sizeof(*o);    // the struct of robj

    switch(o->type)
    {
    case OBJ_STRING:
        if (o->encoding == OBJ_ENCODING_INT)
        {
            return mem;
        }
        else if (o->encoding == OBJ_ENCODING_RAW || o->encoding == OBJ_ENCODING_EMBSTR)
        {
            mem += sdsAllocSize(o->ptr);
            return mem;
        }
        break;

    case OBJ_LIST:
        if (o->encoding == OBJ_ENCODING_QUICKLIST)
        {
            mem += estimate_quicklist(o);
            return mem;
        }        
        break;

    case OBJ_SET:
        if (o->encoding == OBJ_ENCODING_INTSET)
        {
            intset *is = o->ptr;
            mem += intsetBlobLen(is);
            return mem;
        }
        else if (o->encoding == OBJ_ENCODING_HT)
        {
            dict *d = o->ptr;
            mem += estimate_hash(d, 0);
            return mem;
        }
        break;

    case OBJ_HASH:
        if (o->encoding == OBJ_ENCODING_HT)
        {
            dict *d = o->ptr;
            mem += estimate_hash(d, 1);
            return mem;
        }
        else if (o->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl = o->ptr;
            mem += ziplistBlobLen(zl);
            return mem;
        }
        break;

    case OBJ_ZSET:
        if (o->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl  = o->ptr;
            mem += ziplistBlobLen(zl);
            return mem;
        }
        else if (o->encoding == OBJ_ENCODING_SKIPLIST)
        {
            zset *zs = o->ptr;
            mem += sizeof(*zs);

            dict *zs_dict = zs->dict;
            mem += estimate_hash(zs_dict, 0);

            zskiplist *zsl = zs->zsl;
            mem += estimate_zskiplist(zsl);

            return mem;
        }
        break;

    default:
        break;
    }

    serverPanic("estimate_mem_for_object(), unknow type = %d or encoding = %d",
                (int)o->type, (int)o->encoding);
    return 0;
}


static sds marshal_str_int(const robj *o, sds s)
{
    long long val = (long long)o->ptr;
    s = sdscatlen(s, &val, 8);
    return s;
}


static size_t cal_room_str_int(const robj *o)
{
    UNUSED(o);
    return 8;
}

static robj* unmarshal_str_int(const char *buf, const size_t sz)
{
    serverAssert(sz == 8);
    long long val = *((long long*)buf);
    return createStringObjectFromLongLong(val);
}

/*
inline int is_evict_hash_value(const sds v)
{
    return v != shared.hash_rock_val_for_field;
}
*/

/* Check a client is in the state waiting for rock value.
 * Return 1 if the client is waiting some rock value.
 * Otherwise return 0.
 */


/*
static sds debug_random_sds(const int max_len)
{
    int rand_len = random() % max_len;

    sds s = sdsempty();
    s = sdsMakeRoomFor(s, max_len);
    for (int i = 0; i < rand_len; ++i)
    {
        const char c = 'a' + (random() % 26);
        s = sdscatlen(s, &c, 1);
    }
    return s;
}
*/

static void debug_print_pipe_fd(const int must_exit)
{
    serverLog(LL_WARNING, "pid = %d, req[0] = %d, req[1] = %d, res[0] = %d, res[1  = %d",
                          getpid(), pipe_request[0], pipe_request[1], pipe_response[0], pipe_response[1]);
    if (must_exit)
        exit(1);
}

/* Called in service thread.
 *
 * When rdb/aof service thread exit, it needs to call here to clear all resources
 */
static void clear_when_service_thread_exit()
{
    serverAssert(snapshot != NULL);
    rocksdb_release_snapshot(rockdb, snapshot);
    snapshot = NULL;

    clear_resource_pipe(1);

    serverAssert(pthread_mutex_lock(&mutex) == 0);
    service_thread_is_running = 0;

}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#pragma GCC diagnostic pop
