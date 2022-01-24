/* RedRock is based on Redis, coded by Tony. The copyright is same as Redis.
 *
 * Copyright (c) 2018, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "rock_read.h"
#include "rock.h"
#include "server.h"
#include "sds.h"
#include "rock_marshal.h"
#include "rock_write.h"
#include "rock_hash.h"
#include "rock_evict.h"


#ifdef RED_ROCK_MUTEX_DEBUG
static pthread_mutexattr_t mattr_read;
static pthread_mutex_t mutex_read;
#else
static pthread_mutex_t mutex_read = PTHREAD_MUTEX_INITIALIZER;     
#endif

inline static void rock_r_lock() 
{
    serverAssert(pthread_mutex_lock(&mutex_read) == 0);
}
   
inline static void rock_r_unlock() 
{
    serverAssert(pthread_mutex_unlock(&mutex_read) == 0);
}

pthread_t rock_read_thread_id;


/*
 * The critical data is a hash table and array of tasks (task key and return value)
 *
 * 1. For hash table, i.e., read_rock_key_candidates,
 *    key is the rock_key and value is a list of clients(client_id) waiting for the key
 *    NOTE1: List may be NULL before the key is deleted from read_rock_key_candidates
 *          when return task needs to join all lists 
 *    NOTE2: For one key, it is possible to have more than 1 same client
 *           because client use transaction or just MGET k1, k1 ...
 * 
 * 2. For array of tasks, i.e., read_key_tasks and read_return_vals and task_status,
 *    read_key_tasks is the tasks for the read thread. (from the beginning until NULL)
 * 
 * NOTE1: read thread needs to copy tasks to avoid data race when read from RocksDB.
 *
 * NOTE2: Task is the rock key to read. 
 *        It points to the hash table key (same as the one in read_rock_key_candidates)
 *        So if a key is removed from read_rock_key_candidates, 
 *        it is needed to remove from the array first by setting the slot to NULL.
 * 
 * When main thread finishes assigning tasks, it sets task_status to READ_START_TASK
 *      and the read thread will loop to check task_status and can start to work.
 *      NOTE: if zero task, keep the task_status to READ_RETURN_TASK.
 * 
 * When read thread finishes reading from RocksDB, 
 *      it sets task_status to READ_RETURN_TASK
 *      and signals the main thread to recover data.
 * 
 * When main thread recover data, it will check whether it can recover 
 *      because the key may be deleted, modified by other clients 
 *      from Redis DB by other clients beforehand.
 *      Reover condition is that the val is still of rock key val.
 *      Main thread guarantees not evict any key which has been already in candidates
 *      to avoid recover the not-match key (because key could've be regenerated).
 *      Reference rock_write.c for this information.
 * 
 * NOTE: Before key go to candidates, it needs to check write ring buffer first.  
 */

static void key_in_rock_destructor(void *privdata, void *obj) 
{
    UNUSED(privdata);

    serverAssert(obj);
    sdsfree(obj);
}

static void val_as_list_destructor(void *privdata, void *obj) 
{
    UNUSED(privdata);
    
    serverAssert(obj);    
    serverAssert(listLength((list*)obj) == 0);

    listRelease(obj);
}

/* rock key hash table. The key is a rock key. The value is a list of client id */
dictType readCandidatesDictType = 
{
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    key_in_rock_destructor,     /* key destructor, we stored the rock_key ownered by this dict */
    val_as_list_destructor,     /* val destructor for the list of client ids. NOTE: may be NULL */
    NULL                        /* allow to expand */
};

static dict* read_rock_key_candidates = NULL;

#define READ_START_TASK   1
#define READ_RETURN_TASK  2
static int task_status = READ_RETURN_TASK;

#define READ_TOTAL_LEN  8
static sds read_key_tasks[READ_TOTAL_LEN] __attribute__((aligned(64)));     // friend to cpu cache line
static sds read_return_vals[READ_TOTAL_LEN] __attribute__((aligned(64)));

/* We use pipe to signal main thread
 */
static int rock_pipe_read = 0;
static int rock_pipe_write = 0;

// declaration for the following code for init_rock_pipe()
static void on_recover_data(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
static void init_rock_pipe()
{
    int pipefds[2];

    if (pipe(pipefds) == -1) 
        serverPanic("Can not create pipe for rock.");

    rock_pipe_read = pipefds[0];
    rock_pipe_write = pipefds[1];

    if (aeCreateFileEvent(server.el, rock_pipe_read, 
                          AE_READABLE, on_recover_data, NULL) == AE_ERR) 
        serverPanic("Unrecoverable error creating server.rock_pipe file event.");
}

/* Called in read thread to pick read tasks 
 * by copyinng the keys (but not duplicating).
 * The return is the number of the copy rock keys to read from RocksDB.
 */
static int pick_tasks(sds *copy_rock_keys)
{
    rock_r_lock();

    if (task_status == READ_RETURN_TASK)
    {
        // no task or main thread is late to recover data
        rock_r_unlock();
        return 0;     
    }

    int cnt = 0;
    for (int i = 0; i < READ_TOTAL_LEN; ++i)
    {
        const sds task = read_key_tasks[i];

        if (task == NULL)   // the end of this batch of tasks
        {
            serverAssert(read_return_vals[i] == NULL);
            break;
        }

        copy_rock_keys[cnt] = task;     // copy but not duplicate        
        ++cnt;
    }
    
    rock_r_unlock();

    serverAssert(cnt > 0);
    return cnt;
}

/* Work in read thead to read values for keys (rock key).
 * The caller guarantees not in lock mode.
 * NOTE: no need to work in lock mode because keys is copied from read_key_tasks
 */
static void read_from_rocksdb(const int cnt, const sds *keys, sds *vals)
{
    char* errs[READ_TOTAL_LEN];
    size_t rockdb_key_sizes[READ_TOTAL_LEN];
    char* rockdb_vals[READ_TOTAL_LEN];
    size_t rockdb_val_sizes[READ_TOTAL_LEN];

    for (int i = 0; i < cnt; ++i)
    {
        rockdb_key_sizes[i] = sdslen(keys[i]);
        errs[i] = NULL;
    }

    // for manual debug
    // serverLog(LL_WARNING, "read thread read rocksdb start (sleep for 30 seconds) ...");
    // sleep(30);
    // serverLog(LL_WARNING, "read thread read rocksdb end!!!!!");

    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    rocksdb_multi_get(rockdb, readoptions, cnt, 
                      (const char* const *)keys, rockdb_key_sizes, 
                      rockdb_vals, rockdb_val_sizes, errs);
    rocksdb_readoptions_destroy(readoptions);

    for (int i = 0; i < cnt; ++i)
    {
        if (errs[i])
            serverPanic("read_from_rocksdb() reading from RocksDB failed, err = %s, key = %s", errs[i], keys[i]);

        if (rockdb_vals[i] == NULL)
        {
            // not found. 
            // It is illegal but the main thread will handle it (by serverPanic) later.
            vals[i] = NULL;     
        }
        else
        {
            vals[i] = sdsnewlen(rockdb_vals[i], rockdb_val_sizes[i]);
            // free the malloc memory from RocksDB API
            rocksdb_free(rockdb_vals[i]);        
        }
    }
}

/* Called in read thread in an infinite loop.
 * Return 0 means no task,
 * indicating the read thread needs to hava a sleep for a while.
 * Otherwise return 1 indicating there are some tasks 
 * and the tasks have been done,
 * i.e., changing READ_START_TASK to READ_RETURN_TASK.
 */
static int do_tasks()
{
    sds tasks[READ_TOTAL_LEN];
    const int task_num = pick_tasks(tasks);
    if (task_num == 0)
        return 0;

    sds vals[READ_TOTAL_LEN];
    read_from_rocksdb(task_num, tasks, vals);

    rock_r_lock();
    for (int i = 0; i < task_num; ++i)
    {
        serverAssert(read_return_vals[i] == NULL);
        read_return_vals[i] = vals[i];      // transfer the val ownership
    }
    serverAssert(task_status == READ_START_TASK);
    // let pick_tasks() return 0 for the read thread
    task_status = READ_RETURN_TASK; 
    rock_r_unlock();

    // signal main thread to recover data
    const char temp_buf[1] = "a";
    serverAssert(write(rock_pipe_write, temp_buf, 1) == 1);

    return 1;
}

/*
 * The main entry for the read thread
 */
#define MIN_SLEEP_MICRO     16
#define MAX_SLEEP_MICRO     1024            // max sleep for 1 ms
static void* rock_read_main(void* arg)
{
    UNUSED(arg);

    int loop = 0;
    while (loop == 0)
        atomicGet(rock_threads_loop_forever, loop);
        
    unsigned int sleep_us = MIN_SLEEP_MICRO;
    while(loop)
    {
        if (do_tasks() != 0)
        {
            sleep_us = MIN_SLEEP_MICRO;     // if we have task, shorten the sleep time
            atomicGet(rock_threads_loop_forever, loop);
            continue;       // no sleep, go on for more task
        }        

        usleep(sleep_us);
        sleep_us <<= 1;        // double sleep time
        if (sleep_us > MAX_SLEEP_MICRO) 
            sleep_us = MAX_SLEEP_MICRO;

        atomicGet(rock_threads_loop_forever, loop);
    }

    return NULL;
}

/* Called in main thread to get more keys (Up to READ_TOTAL_LEN) for task assignment.
 * The caller guarantees in lock mode.
 * Copy (but not duplicate) the keys from read_rock_key_candidates for effiency.
 * Return the number of the selected keys.
 */
static int get_keys_from_candidates_before_assignment(sds* rock_keys)
{
    int cnt = 0;

    dictIterator *di = dictGetIterator(read_rock_key_candidates);
    dictEntry *de;
    while ((de = dictNext(di))) 
    {
        sds rock_key = dictGetKey(de);
        rock_keys[cnt] = rock_key;
        ++cnt;
        if (cnt == READ_TOTAL_LEN)
            break;
    }
    dictReleaseIterator(di);

    return cnt; 
}

/* Called in main thread to assgin tasks.
 * The caller guarantees in lock mode.
 * NOTE: read_key_tasks will have the same pointer to keys in read_rock_key_candidates
 *       so the caller needs to guarantee safety of keys.
 */
static void assign_tasks(const int cnt, const sds* tasks)
{
    serverAssert(cnt > 0);
    serverAssert(read_key_tasks[0] == NULL);    // tasks must be empty
    serverAssert(task_status != READ_START_TASK);

    for (int i = 0; i < cnt; ++i)
    {
        read_key_tasks[i] = tasks[i];
        serverAssert(read_return_vals[i] == NULL);  // the val resource must be reclaimed already 
    }
    task_status = READ_START_TASK;  // let read thread to work
}

/* Called in main thread to assign tasks.
 * The caller guarantee in lock mode.
 * If read thread is working or no task is available, no need to assgin task.
 */
static void try_assign_tasks()
{
    if (task_status == READ_START_TASK)
        return;   // read thread is working, can not assign task

    if (read_key_tasks[0] != NULL)
        return;     // main thread has not response for the signal of read thread

    sds tasks[READ_TOTAL_LEN];
    const int avail = get_keys_from_candidates_before_assignment(tasks);
    if (avail != 0)
        assign_tasks(avail, tasks);    // change task_status to READ_START_TASK
    // else{}, need to keep task_status to READ_RETURN_TASK
}

/* Called by main thraed because c->rock_key_num changes to zero,
 * i.e., not is_client_in_waiting_rock_value_state().
 *
 * But because it is called by async mode (from RocksDB recovering), 
 * so we need check again (by calling processCommandAndResetClient() again)
 * because some other keys may be evicted to RocksDB during the async period.
 * 
 * The caller guaratee not in lock mode.
 */
static void resume_command_for_client_in_async_mode(client *c)
{
    int processCommandAndResetClient(client *c, const int rock_async_re_entry);        // networkng.c, no declaration in any header

    const int ret = processCommandAndResetClient(c, 1);     // call again for asyc mode

    if (ret != C_ERR)
    {
        // if the client is still valid after processCommandAndResetClient()
        // try to read buffer 
        // NOTE: if the client is still in waiting rock value state,
        //       it will skip process socket buffer in processInputBuffer()
        if (c->querybuf && sdslen(c->querybuf) > 0) 
            processInputBuffer(c);
    }
}

/* Called in main thread.
 *
 * If the recover redis key exist in redis db and has rock val, recover it.
 * It means the recover_val is just for the key.
 * 
 * If not, the key may be deleted or regenerated for the async mode.
 */
static void try_recover_val_object_in_redis_db(const int dbid, const sds recover_val,
                                               const char *redis_key, const size_t redis_key_len)
                                               
{
    if (recover_val == NULL)
        // deal with read thread not found error later here
        serverPanic("try_recover_val_object_in_redis_db() the recover_val is NULL(not found) for redis key = %s, dbid = %d", 
                    redis_key, dbid);

    sds key = sdsnewlen(redis_key, redis_key_len);

    redisDb *db = server.db + dbid;
    dictEntry *de = dictFind(db->dict, key);
    if (de != NULL)
    {
        const robj *o = dictGetVal(de);
        if (is_rock_value(o))
        {
            #if defined RED_ROCK_DEBUG
            serverAssert(debug_check_type(recover_val, o));
            #endif
            const sds internal_key = dictGetKey(de);      
            dictGetVal(de) = unmarshal_object(recover_val);    
            on_recover_key_for_rock_evict(dbid, internal_key);
        }
    }

    sdsfree(key);
}

/* Called in main thread.
 * The caller guarantee in lcok mode.
 *
 * If the recover a hash field exist and has rock value, recover it.
 * It means the recover_val is just for the hash key and field.
 * 
 * Otherwise do nothing becausee 
 * 1. the key may be deleted or regenerated
 * 2. the field may be deleted or regenerated
 * for the async mode.
 */
static void try_recover_field_in_hash(const int dbid, const sds recover_val,
                                      const char *input_hash_key, const size_t input_hash_key_len,
                                      const char *input_hash_field, const size_t input_hash_field_len)
{
    if (recover_val == NULL)
        // deal with read thread not found error later here
        serverPanic("try_recover_field_in_hash() the recover_val is NULL(not found) for hash key = %s, hash_field = %s, dbid = %d", 
                    input_hash_key, input_hash_field, dbid);

    sds hash_key = sdsnewlen(input_hash_key, input_hash_key_len);
    sds hash_field = sdsnewlen(input_hash_field, input_hash_field_len);

    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, hash_key);
    if (de_db == NULL)
        goto reclaim;   // the hash key may be deleted by other client
    
    robj *o = dictGetVal(de_db);
    if (!(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT))
        goto reclaim;   // the hash key may be overwritten by other client     

    if (is_rock_value(o))
        // the hash key could be deleted, then regenerated, 
        // then as a whole key to be evicted to RocksDB
        goto reclaim;    
    
    dict *hash = o->ptr;
    dictEntry *de_hash = dictFind(hash, hash_field);
    if (de_hash == NULL)
        goto reclaim;   // the field may be deleted by other cliient
    
    sds val = dictGetVal(de_hash);
    if (val != shared.hash_rock_val_for_field)
        goto reclaim;   // the field's value may be overwritten by other cliient

    // NOTE: we need a copy of recover_val for the recover
    //       because the caller will reclaim recover_val, check recover_data()
    const sds copy_val = sdsdup(recover_val);
    dictGetVal(de_hash) = copy_val;
    on_recover_field_of_hash(dbid, hash_key, hash_field);

reclaim:
    sdsfree(hash_key);
    sdsfree(hash_field);
}

/* Called in main thread.
 * Join the clients for current task to waiting_clients
 * The caller guarantee in lock mode.
 *
 * NOTE: Do not release the list for the candidate 
 *       because it will be freed whhen dictDelete() in the top caller.
 */
static void join_waiting_clients(const sds task, list **waiting_clients)
{
    dictEntry *de = dictFind(read_rock_key_candidates, task);
    serverAssert(de && dictGetKey(de) == task);

    list *candidate_list = dictGetVal(de);
    serverAssert(candidate_list && listLength(candidate_list) > 0);
    listJoin(*waiting_clients, candidate_list);
}

/* Called in main thread to recover one key for redis db
 * The caller guarantee in lcok mode.
 *
 * It will try to recover the obj in DB if it can
 * because the key could be deleted or regenerated in async mode.
 * 
 * The client list will join to waiting_clients 
 * and thus will be set empty in read_rock_key_candidates
 */
static void recover_data_for_db(const sds task,
                                const sds recover_val,
                                list **waiting_clients)
{
    int dbid;
    const char *redis_key;
    size_t redis_key_len;
    decode_rock_key_for_db(task, &dbid, &redis_key, &redis_key_len);

    try_recover_val_object_in_redis_db(dbid, recover_val, redis_key, redis_key_len);

    join_waiting_clients(task, waiting_clients);
}

static void recover_data_for_hash(const sds task,
                                 const sds recover_val,
                                 list **waiting_clients)
{
    int dbid;
    const char *hash_key;
    size_t hash_key_len;
    const char *hash_field;
    size_t hash_field_len;
    decode_rock_key_for_hash(task, &dbid, &hash_key, &hash_key_len, &hash_field, &hash_field_len);

    try_recover_field_in_hash(dbid, recover_val, hash_key, hash_key_len, hash_field, hash_field_len);

    join_waiting_clients(task, waiting_clients);
}

/* Called in main thread.
 *
 * NNTE: The caller guaranteees not in lock mode. 
 * 
 * When some rock keys are recovered or the key is deleted or regenerated
 * which means it guarantee no rock value for the keys,
 * the clients (by joininng together) waiting associated with the rock keys 
 * will be checked whether they will be resumed again. 
 *
 * NOTE1: client may be invalid because the client id 
 *        won't be deleted in read_rock_key_candidates 
 *        while the client has closed a Redis connection.
 *
 * NOTE2: client id may be duplicated in client_ids 
 *        for case of multi keys recovered like get <key> <key> or transaction.
 */
static void check_client_resume_after_recover_data(const list *client_ids)
{
    listIter li;
    listNode *ln;
    listRewind((list*)client_ids, &li);
    while ((ln = listNext(&li)))
    {
        uint64_t client_id = (uint64_t)listNodeValue(ln);
        client *c = lookup_client_from_id(client_id);
        if (c)
        {
            serverAssert(c->rock_key_num > 0);
            --c->rock_key_num;
            if (!is_client_in_waiting_rock_value_state(c))
                resume_command_for_client_in_async_mode(c);
        }
    }
}

/* Called in main thread to recover the data from RocksDB.
 *
 * For every finished task, we try to recover the val in Redis DB,
 * because in async mode, the key could be deleted or regenerated.
 * 
 * Then we set task array to NULL, delete the finished tasks in candidates,
 *      reclaim all resouce and try to assign new tasks.
 * 
 * And we need join all waiting client ids for all finished tasks.
 * For the joining waiting clients, check whether they are ready 
 * for processing the command again because some values are restored
 */
static void recover_data()
{
    list *waiting_clients = listCreate();

    rock_r_lock();
    serverAssert(task_status == READ_RETURN_TASK);
    serverAssert(read_key_tasks[0] != NULL);
    for (int i = 0; i < READ_TOTAL_LEN; ++i)
    {
        const sds task = read_key_tasks[i];
        if (task == NULL)
            break;  // the end of task array

        // join list will happen in recover_data_for_XX()
        if (task[0] == ROCK_KEY_FOR_DB)
        {
            recover_data_for_db(task, read_return_vals[i], &waiting_clients);
        }
        else
        {
            serverAssert(task[0] == ROCK_KEY_FOR_HASH);
            recover_data_for_hash(task, read_return_vals[i], &waiting_clients);
        }
        
        // must set NULL for next batch task assignment, like try_assign_tasks() and read thread loop
        read_key_tasks[i] = NULL;       // keys will be released by the following dictDelete()
        sdsfree(read_return_vals[i]);
        read_return_vals[i] = NULL;

        serverAssert(dictDelete(read_rock_key_candidates, task) == DICT_OK);
    }
    try_assign_tasks();
    rock_r_unlock();

    // NOTE: not in lock mode to call check_client_resume_after_recover_data()
    check_client_resume_after_recover_data(waiting_clients);    

    listRelease(waiting_clients);
}

/* Main thread response the pipe signal from read thread
 * which indicates the tasks are finished */
static void on_recover_data(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) 
{
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    UNUSED(fd);

    // clear pipe signal
    char tmp_use_buf[1];
    serverAssert(read(rock_pipe_read, tmp_use_buf, 1) == 1);     

    recover_data();
}

/* Called in main thread.
 * After the check for ring buffer for db key, 
 * it goes on to recover value from RocksDB in async way.
 * 
 * NOTE: redis_keys will be duplicated for rock key format and saved in candidates.
 *       So the caller deals with the resource of redis_keys independently.
 */
static void go_on_need_rock_keys_from_rocksdb(const uint64_t client_id, 
                                              const int dbid, const list *redis_keys)
{
    serverAssert(listLength(redis_keys) > 0);

    listIter li;
    listNode *ln;
    listRewind((list*)redis_keys, &li);

    rock_r_lock();

    int added = 0;
    while ((ln = listNext(&li)))
    {
        const sds redis_key = listNodeValue(ln);

        sds rock_key = sdsdup(redis_key);
        rock_key = encode_rock_key_for_db(dbid, rock_key);

        dictEntry *de = dictFind(read_rock_key_candidates, rock_key);
        if (de == NULL)
        {
            list *client_ids = listCreate();
            listAddNodeHead(client_ids, (void*)client_id);  
            // transfer ownership of rock_key and client_ids to read_rock_key_candidates
            dictAdd(read_rock_key_candidates, rock_key, client_ids);    
            added = 1;
        }
        else
        {
            list *client_ids = dictGetVal(de);
            serverAssert(listLength(client_ids) > 0);
            listAddNodeTail(client_ids, (void*)client_id);
            sdsfree(rock_key);
        }
    }

    if (added)
        try_assign_tasks();

    rock_r_unlock();
}

/* From redis_keys, direct read from RocksDB and recoover them in redis db in sync moode */
static void direct_recover_rock_keys_from_rocksdb(const int dbid, const list *redis_keys)
{
    serverAssert(listLength(redis_keys) > 0);

    redisDb *db = server.db + dbid;

    listIter li;
    listNode *ln;
    listRewind((list*)redis_keys, &li);

    while ((ln = listNext(&li)))
    {
        const sds redis_key = listNodeValue(ln);

        sds rock_key = sdsdup(redis_key);
        rock_key = encode_rock_key_for_db(dbid, rock_key);

        size_t db_val_len;
        char *err = NULL;
        rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
        char *db_val = rocksdb_get(rockdb, readoptions, rock_key, sdslen(rock_key), &db_val_len, &err);
        rocksdb_readoptions_destroy(readoptions);

        if (err)
            serverPanic("direct_recover_rock_keys_from_rocksdb(), err = %s", err);

        if (db_val == NULL)
            // NOT FOUND, it is illegal
            serverPanic("direct_recover_rock_keys_from_rocksdb(), not found, redis_key = %s", redis_key);

        sds recover_val = sdsnewlen(db_val, db_val_len);
        rocksdb_free(db_val);

        robj *recover_o = unmarshal_object(recover_val);
        sdsfree(recover_val);

        dictEntry *de_db = dictFind(db->dict, redis_key);
        serverAssert(de_db);
        if (is_rock_value(dictGetVal(de_db)))
        {
            // NOTE: same redis_key could be repeated in redis_keys
            // First one win the recover
            dictGetVal(de_db) = recover_o;
            on_recover_key_for_rock_evict(dbid, dictGetKey(de_db));
        }
        else
        {
            decrRefCount(recover_o);
        }

        sdsfree(rock_key);
    }
}

/* Called in main thread.
 * After the check for ring buffer for hash,
 * it goes on to recover value from RocksDB in async way.
 * 
 * NOTE: hash_keys and hash_fields will be duplicated for rock key format and saved in candidates.
 *       So the caller deals with the resource of redis_keys independently.
 */
static void go_on_need_rock_hashes_from_rocksdb(const uint64_t client_id, const int dbid, 
                                                const list *hash_keys, const list *hash_fields)
{
    serverAssert(listLength(hash_keys) > 0 && listLength(hash_keys) == listLength(hash_fields));

    listIter li_key;
    listNode *ln_key;
    listIter li_field;
    listNode *ln_field;
    listRewind((list*)hash_keys, &li_key);
    listRewind((list*)hash_fields, &li_field);

    rock_r_lock();

    int added = 0;
    while ((ln_key = listNext(&li_key)))
    {
        ln_field = listNext(&li_field);

        const sds hash_key = listNodeValue(ln_key);
        const sds hash_field = listNodeValue(ln_field);

        sds rock_key = sdsdup(hash_key);
        rock_key = encode_rock_key_for_hash(dbid, rock_key, hash_field);

        dictEntry *de = dictFind(read_rock_key_candidates, rock_key);
        if (de == NULL)
        {
            list *client_ids = listCreate();
            listAddNodeHead(client_ids, (void*)client_id);  
            // transfer ownership of rock_key and client_ids to read_rock_key_candidates
            dictAdd(read_rock_key_candidates, rock_key, client_ids);    
            added = 1;
        }
        else
        {
            list *client_ids = dictGetVal(de);
            serverAssert(listLength(client_ids) > 0);
            listAddNodeTail(client_ids, (void*)client_id);
            sdsfree(rock_key);
        }
    }

    if (added)
        try_assign_tasks();

    rock_r_unlock();
}

/* From hash_keys & hash_fields, direct read from RocksDB and recoover them in redis db in sync moode */
static void direct_recover_rock_fields_from_rocksdb(const int dbid, const list *hash_keys, const list *hash_fields)
{
    serverAssert(listLength(hash_keys) > 0 && listLength(hash_keys) == listLength(hash_fields));

    redisDb *db = server.db + dbid;

    listIter li_key;
    listNode *ln_key;
    listIter li_field;
    listNode *ln_field;
    listRewind((list*)hash_keys, &li_key);
    listRewind((list*)hash_fields, &li_field);

    while ((ln_key = listNext(&li_key)))
    {
        ln_field = listNext(&li_field);

        const sds hash_key = listNodeValue(ln_key);
        const sds hash_field = listNodeValue(ln_field);

        sds rock_key = sdsdup(hash_key);
        rock_key = encode_rock_key_for_hash(dbid, rock_key, hash_field);

        size_t db_val_len;
        char *err = NULL;
        rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
        char *db_val = rocksdb_get(rockdb, readoptions, rock_key, sdslen(rock_key), &db_val_len, &err);
        rocksdb_readoptions_destroy(readoptions);

        if (err)
            serverPanic("direct_recover_rock_fields_from_rocksdb(), err = %s", err);

        if (db_val == NULL)
            // NOT FOUND, it is illegal
            serverPanic("direct_recover_rock_fields_from_rocksdb(), not found, hash_key = %s, hash_field = %s", 
                        hash_key, hash_field);

        sds recover_val = sdsnewlen(db_val, db_val_len);
        rocksdb_free(db_val);

        dictEntry *de_db = dictFind(db->dict, hash_key);
        serverAssert(de_db);
        robj *o_db = dictGetVal(de_db);
        serverAssert(o_db->type == OBJ_HASH && o_db->encoding == OBJ_ENCODING_HT);
        dict *hash = o_db->ptr;
        dictEntry *de_hash = dictFind(hash, hash_field);
        serverAssert(de_hash);
        if (dictGetVal(de_hash) == shared.hash_rock_val_for_field)
        {
            // NOTE: same hash_key and hash_field could be repeated in the input
            // First one win the recover
            dictGetVal(de_hash) = recover_val;
            on_recover_field_of_hash(dbid, hash_key, hash_field);
        }
        else
        {
            sdsfree(recover_val);
        }

        sdsfree(rock_key);
    }
}


/* If some key already in ring buf, recover them in Redis DB.
 * Return a list for un-recovered keys (may be empty if all redis_keys in ring buffer)
 * If no key in ring buf, return NULL. 
 *
 * The caller guarantee not in lock mode.
 *
 * NOTE1: the un-rocovered keys share the sds pointer 
 *        as the one from the input argument of redis_keys.
 * 
 * NOTE2: same key could repeat in redis_keys.
 * 
 * NOET3: because the async mode, one command for one client could call here for serveral times,
 *        so the keyspace could change.
 */
static list* check_ring_buf_first_and_recover_for_db(const int dbid, const list *redis_keys)
{
    // Call the API for ring buf in rock_write.c
    list *vals = get_vals_from_write_ring_buf_first_for_db(dbid, redis_keys);
    if (vals == NULL)
        return NULL;

    serverAssert(listLength(vals) == listLength(redis_keys));

    list *left = listCreate();

    redisDb *db = server.db + dbid;

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
            {
                const sds internal_key = dictGetKey(de);      
                // NOTE: the same key could repeat in redis_keys
                //       so the second duplicated key, we can not guaratee it is rock value
                dictGetVal(de) = unmarshal_object(recover_val);     // revocer in redis db
                on_recover_key_for_rock_evict(dbid, internal_key);
            }
        }
    }

    // reclaim the resource of vals which are allocated 
    // in get_vals_from_write_ring_buf_first()
    listSetFreeMethod(vals, (void (*)(void*))sdsfree);
    listRelease(vals);

    return left;
}

/* If some hash key and field already in ring buf, recover them in Redis DB.
 * Return is for left_keys and left_fields as
 * lists for un-recovered keys and fiields (may be empty if all redis_keys in ring buffer)
 * If no key in ring buf, both are NULL. 
 *
 * The caller guarantee not in lock mode.
 *
 * NOTE1: the left_keys and  left_fields share the sds pointer 
 *       as hash_keys and hash_fields.
 * 
 * NOTE2: same key and field could repeat in redis_keys.
 * 
 * NOET3: because the async mode, one command for one client could call here for serveral times,
 *        so the keyspace could change.
 */
static void check_ring_buf_first_and_recover_for_hash(const int dbid,
                                                      const list *hash_keys, const list *hash_fields,
                                                      list **unrecovered_keys, list **unrecovered_fields)
{
    serverAssert(*unrecovered_keys == NULL && *unrecovered_fields == NULL);

    // Call the API for ring buf in rock_write.c
    list *vals = get_vals_from_write_ring_buf_first_for_hash(dbid, hash_keys, hash_fields);
    if (vals == NULL)
        return;

    serverAssert(listLength(vals) == listLength(hash_keys));

    list *left_keys = listCreate();
    list *left_fields = listCreate();

    redisDb *db = server.db + dbid;

    listIter li_vals;
    listNode *ln_vals;
    listIter li_keys;
    listNode *ln_keys;
    listIter li_fields;
    listNode *ln_fields;
    listRewind(vals, &li_vals);
    listRewind((list*)hash_keys, &li_keys);
    listRewind((list*)hash_fields, &li_fields);

    while ((ln_vals = listNext(&li_vals)))
    {
        ln_keys = listNext(&li_keys);
        ln_fields = listNext(&li_fields);

        const sds hash_key = listNodeValue(ln_keys);
        const sds hash_field = listNodeValue(ln_fields);

        const sds recover_val = listNodeValue(ln_vals);
         if (recover_val == NULL)
         {
            // not found in ring buffer
            listAddNodeTail(left_keys, hash_key);
            listAddNodeTail(left_fields, hash_field);
         }
         else
         {
            // need to recover data which is from ring buffer
            // NOTE: no need to deal with rock_key_num in client. The caller will take care
            dictEntry *de_db = dictFind(db->dict, hash_key);
            // the redis_key must be here because it is from
            // the caller on_client_need_rock_keys_for_hash() which guaratee this
            serverAssert(de_db);
            robj *o = dictGetVal(de_db);
            dict *hash = o->ptr;
            dictEntry *de_hash = dictFind(hash, hash_field);
            // same guarantee from the caller on_client_need_rock_keys_for_hash()
            serverAssert(de_hash);
            
            if (dictGetVal(de_hash) == shared.hash_rock_val_for_field)      
            {
                // NOTE: the same key&field could repeat in hash_keys and hash_fields
                dictGetVal(de_hash) = recover_val;     // revocer in redis db
                on_recover_field_of_hash(dbid, hash_key, hash_field);
                listNodeValue(ln_vals) = NULL;      // avoid reclaim in the end of the fuction
            }
         }
    }

    // reclaim the resource of vals which are allocated 
    // in get_vals_from_write_ring_buf_first() 
    // NOTE: the ONLY ONE recover val's ownership has been transfered to the hash in redis db
    listSetFreeMethod(vals, (void (*)(void*))sdsfree);
    listRelease(vals);   

    *unrecovered_keys = left_keys;
    *unrecovered_fields = left_fields;
}

#if defined RED_ROCK_DEBUG
static void debug_check_all_db_keys_are_rock_value(const int dbid, const list *redis_keys)
{
    redisDb *db = server.db + dbid;
    listIter li;
    listNode *ln;
    listRewind((list*)redis_keys, &li);
    while ((ln = listNext(&li)))
    {
        sds key = listNodeValue(ln);
        dictEntry *de = dictFind(db->dict, key);
        serverAssert(de);
        if (!is_rock_value(dictGetVal(de)))
            serverPanic("debug_check_all_value_is_rock_value() not rock value, dbid = %d, key = %s",
                        dbid, key);
    }
}
#endif

#if defined RED_ROCK_DEBUG
static void debug_check_all_hash_fields_are_rock_value(const int dbid, 
                                                       const list *hash_keys, const list *hash_fields)
{
    redisDb *db = server.db + dbid;

    listIter li_key;
    listNode *ln_key;
    listRewind((list*)hash_keys, &li_key);
    listIter li_field;
    listNode *ln_field;
    listRewind((list*)hash_fields, &li_field);
    while ((ln_key = listNext(&li_key)))
    {
        ln_field = listNext(&li_field);

        sds key = listNodeValue(ln_key);
        sds field = listNodeValue(ln_field);

        dictEntry *de_db = dictFind(db->dict, key);
        serverAssert(de_db);
        serverAssert(!is_rock_value(dictGetVal(de_db)));
        robj *o = dictGetVal(de_db);
        dict *hash = o->ptr;
        dictEntry *de_hash = dictFind(hash, field);
        serverAssert(de_hash);
        sds val = dictGetVal(de_hash);
        serverAssert(val == shared.hash_rock_val_for_field);
    }
}
#endif

/* Called in main thread when a client finds it needs some redis keys to
 * continue for a command because the whole key's value is in RocksDB.
 * The caller guarantee not using read lock.
 * 
 * The client's rock_key_num guarantees zero before calling
 * and we will calculate it and set it in this function.
 * 
 * The redis_keys is the list of keys needed by the command (i.e., with rock value)
 * NOTE: redis key could be repeated (e.g., transaction or just mget k1 k1 ...)
 * 
 * First, we need check ring buffer to recover the keys if exist.
 * 
 * After the ring buffer recovering, if some keys left 
 * which are needed to recover from RocksDB (async mode),
 * we go on to recover them from RocksDB and return 0 meaning it will be for async mode.
 * 
 * Otherwise, return 1 indicating no need for async (NOTE: only for key but not including hash field) 
 * and the caller can execute the command right now (sync mode)
 * if and only if no hash field is needed (check on_client_need_rock_fields_for_hashes).
 * For no transaction, it is easy to decide, 
 * but for transaction it is complicated and we will mix with on_client_need_rock_fields_for_hashes().
 * 
 * The caller needs to reclaim the list after that (which is created by the caller)
 * 
 * NOTE: One client for one command could call this serveral times.
 *       So checking needs consider async situation.
 */
int on_client_need_rock_keys_for_db(client *c, const list *redis_keys)
{
    serverAssert(redis_keys && listLength(redis_keys) > 0);

    #if defined RED_ROCK_DEBUG
    debug_check_all_db_keys_are_rock_value(c->db->id, redis_keys);
    #endif

    serverAssert(c->rock_key_num == 0);
    
    const int dbid = c->db->id;
    const uint64_t client_id = c->id;

    int sync_mode_for_db = 0;
    list *left = check_ring_buf_first_and_recover_for_db(dbid, redis_keys);

    if (left == NULL)
    {
        // nothing found in ring buffer
        c->rock_key_num = listLength(redis_keys);
        go_on_need_rock_keys_from_rocksdb(client_id, dbid, redis_keys);
    }
    else if (listLength(left) == 0)
    {
        // all found in ring buffer
        sync_mode_for_db = 1;
        // NOTE: keep c->rock_key_num == 0 in sync mode
    } 
    else 
    {
        c->rock_key_num = listLength(left);     // set async mode
        go_on_need_rock_keys_from_rocksdb(client_id, dbid, left);
    }

    if (left)
        // The left has the same key in redis_keys, so only release the list
        listRelease(left);      

    return sync_mode_for_db;
}

/* Like the above, but do not set rock_key_num because we use sync mode */
void on_client_need_rock_keys_for_db_in_sync_mode(client *c, const list *redis_keys)
{
    const int dbid = c->db->id;

    list *left = check_ring_buf_first_and_recover_for_db(dbid, redis_keys);

    if (left == NULL)
    {
        // nothing found in ring buffer
        // deal with the total redis_keys from rocksDB directly in sync mode
        direct_recover_rock_keys_from_rocksdb(dbid, redis_keys);
    }
    else if (listLength(left) == 0)
    {
        // all found in ring buffer
    }
    else
    {
        // deal with the left by reading from RocksdB diretly in sync mode
        direct_recover_rock_keys_from_rocksdb(dbid, left);
    }

    if (left)
        listRelease(left);
}

/* Called in main thread when a client finds it needs some hash keys 
 * and hash fields to continue for a command because the field's value is in RocksDB.
 * The caller guarantee not using read lock.
 * 
 * The client's rock_key_num guarantees zero before calling
 * and we will calculate it and set it in this function.
 * 
 * The hash_keys and the hash_fields are the list of keys and fields
 * needed by the command for hash (i.e., field's value with rock value)
 * NOTE: hash key and hash field could be repeated (e.g., transaction or just hmget key f1 f1 ...)
 * 
 * First, we need check ring buffer to recover the hash if exist.
 * 
 * After the ring buffer recovering, if some left 
 * which are needed to recover from RocksDB (async mode),
 * we go on to recover them from RocksDB and return 0 meaning it will be for async mode.
 * 
 * Otherwise, etrun 1 indicating no need for async and
 * the caller can execute the command right now (sync mode)
 * if and only if the client need no rock key (not include these hash keys).
 * For no transaction, it is easy to decide,
 * but for transaction, it is complicated and needs to mix with on_client_need_rock_keys_for_db().
 * 
 * The caller needs to reclaim the list after that (which is created by the caller)
 * 
 * NOTE: One client for one command could call this serveral times.
 *       So checking needs consider async situation.
 */
int on_client_need_rock_fields_for_hashes(client *c, const list *hash_keys, const list *hash_fields)
{
    serverAssert(hash_keys && listLength(hash_keys) > 0 && listLength(hash_keys) == listLength(hash_fields));

    #if defined RED_ROCK_DEBUG
    debug_check_all_hash_fields_are_rock_value(c->db->id, hash_keys, hash_fields);
    #endif

    // we can not serverAssert(c->rock_key_num == 0) because keys are processed
    // before hash and if in transaction, it could be c->rock_key_num != 0

    const int dbid = c->db->id;
    const uint64_t client_id = c->id;

    int sync_mode_for_hash = 0;
    list *left_keys = NULL;
    list *left_fields = NULL;
    check_ring_buf_first_and_recover_for_hash(dbid, hash_keys, hash_fields, &left_keys, &left_fields);

    if (left_keys == NULL)
    {
        // nothing found in ring buffer
        c->rock_key_num += listLength(hash_keys);
        go_on_need_rock_hashes_from_rocksdb(client_id, dbid, hash_keys, hash_fields);
    }
    else if (listLength(left_keys) == 0)
    {
        // all found in ring buffer
        sync_mode_for_hash = 1;
        // keep the previous c->rock_key_num (from db key processing) in sync mode
    }
    else
    {
        c->rock_key_num += listLength(left_keys);
        go_on_need_rock_hashes_from_rocksdb(client_id, dbid, left_keys, left_fields);
    }

    if (left_keys)
    {
        listRelease(left_keys);
        listRelease(left_fields);
    }

    return sync_mode_for_hash;
}

/* Like the above, but do not set rock_key_num because we use sync mode */
void on_client_need_rock_fields_for_hash_in_sync_mode(client *c, const list *hash_keys, const list *hash_fields)
{
    const int dbid = c->db->id;

    list *left_keys = NULL;
    list *left_fields = NULL;
    check_ring_buf_first_and_recover_for_hash(dbid, hash_keys, hash_fields, &left_keys, &left_fields);

    if (left_keys == NULL)
    {
        // nothing found in ring buffer
        // deal with the total hash_keys and hash_fields from RocksDB directly in sync mode
        direct_recover_rock_fields_from_rocksdb(dbid, hash_keys, hash_fields);
    }
    else if (listLength(left_keys) == 0)
    {
        // all found in ring buffer        
    }
    else
    {
        // deal with the left by reading from RocksdB diretly in sync mode
        direct_recover_rock_fields_from_rocksdb(dbid, left_keys, left_fields);
    }

    if (left_keys)
    {
        listRelease(left_keys);
        listRelease(left_fields);
    }
}

/* API for rock_write.c for checking whether the key is in candidates
 * Called in main thread.
 * Return 1 if it is in read_rock_key_candidates. Otherwise 0.
 */
int already_in_candidates_for_db(const int dbid, const sds redis_key)
{
    sds rock_key = sdsdup(redis_key);
    rock_key = encode_rock_key_for_db(dbid, rock_key);

    int exist = 0;
    rock_r_lock();
    if (dictFind(read_rock_key_candidates, rock_key) != NULL)
        exist = 1;
    rock_r_unlock();

    sdsfree(rock_key);

    return exist;
}

/* API for rock_write.c for checking whether the key with the field is in candidates
 * Called in main thread.
 * Return 1 if it is in read_rock_key_candidates. Otherwise 0.
 */
int already_in_candidates_for_hash(const int dbid, const sds redis_key, const sds field)
{
    sds rock_key = sdsdup(redis_key);
    rock_key = encode_rock_key_for_hash(dbid, rock_key, field);

    int exist = 0;
    rock_r_lock();
    if (dictFind(read_rock_key_candidates, rock_key) != NULL)
        exist = 1;
    rock_r_unlock();

    sdsfree(rock_key);

    return exist;
}

/* the API for start the read thread 
 * Call only once in main thread and before the read thread starts
 */
void init_and_start_rock_read_thread()
{
#ifdef RED_ROCK_MUTEX_DEBUG
    serverAssert(pthread_mutexattr_init(&mattr_read) == 0);
    serverAssert(pthread_mutexattr_settype(&mattr_read, PTHREAD_MUTEX_ERRORCHECK) == 0);
    serverAssert(pthread_mutex_init(&mutex_read, &mattr_read) == 0);
#endif

    rock_r_lock();
    read_rock_key_candidates = dictCreate(&readCandidatesDictType, NULL);
    task_status = READ_RETURN_TASK;
    for (int i = 0; i < READ_TOTAL_LEN; ++i)
    {
        read_key_tasks[i] = NULL;
        read_return_vals[i] = NULL;
    }
    rock_r_unlock();

    init_rock_pipe();

    if (pthread_create(&rock_read_thread_id, NULL, rock_read_main, NULL) != 0) 
        serverPanic("Unable to create a rock read thread.");
}

/* Called in main thread for rock.c when main thread will exit */
void join_read_thread()
{
    int s;
    void *res;
    
    s = pthread_join(rock_read_thread_id, &res);
    if (s != 0)
    {
        serverLog(LL_WARNING, "rock read thread join failure. err = %d", s);
    }
    else
    {
        serverLog(LL_NOTICE, "rock read thread exit and join successfully.");
    }
}
