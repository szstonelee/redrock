#include "rock_read.h"
#include "rock.h"
#include "server.h"
#include "sds.h"
#include "rock_marshal.h"
#include "rock_write.h"

/* Write Spin Lock for Apple OS and Linux */
#if defined(__APPLE__)

    #include <os/lock.h>
    static os_unfair_lock r_lock;

    static void init_read_spin_lock() 
    {
        r_lock = OS_UNFAIR_LOCK_INIT;
    }

    inline static void rock_r_lock() 
    {
        os_unfair_lock_lock(&r_lock);
    }

    inline static void rock_r_unlock() 
    {
        os_unfair_lock_unlock(&r_lock);
    }

#else   // Linux

    #include <pthread.h>
    static pthread_spinlock_t r_lock;

    static void init_read_spin_lock() 
    {
        pthread_spin_init(&r_lock, 0);
    }    

    inline static void rock_r_lock() 
    {
        int ret = pthread_spin_lock(&r_lock);
        serverAssert(ret == 0);
    }
   
    inline static void rock_r_unlock() 
    {
        int ret = pthread_spin_unlock(&r_lock);
        serverAssert(ret == 0);
    }

#endif

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
    sdsfree(obj);
}

static void val_as_list_destructor(void *privdata, void *obj) 
{
    UNUSED(privdata);
    if (obj)
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

/* NOTE: Call only once in main thread and before the read thread starts
 */
static void init_rock_read()
{
    init_read_spin_lock();

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
    }

    // serverLog(LL_WARNING, "read thread read rocksdb start (sleep for 10 seconds) ...");
    // sleep(10);
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
    task_status = READ_RETURN_TASK; 
    rock_r_unlock();

    // signal main thread to recover data
    char temp_buf[1] = "a";
    size_t n = write(rock_pipe_write, temp_buf, 1);
    serverAssert(n == 1);

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

    sds tasks[READ_TOTAL_LEN];
    const int avail = get_keys_from_candidates_before_assignment(tasks);
    if (avail != 0)
        assign_tasks(avail, tasks);    // change task_status to READ_START_TASK
    // else{}, need to keep task_status to READ_RETURN_TASK
}

/* Called by main thraed because c->rock_key_num changes to zero.
 * But because it is called by async mode (from RocksDB recovering), 
 * so we need check again (by calling processCommandAndResetClient())
 * because some other keys may be evicted to RocksDB during the async period.
 * The caller guaratee not in lock mode.
 */
int processCommandAndResetClient(client *c);        // networkng.c, no declaration in any header
static void resume_command_for_client_in_async_mode(client *c)
{
    serverAssert(!is_client_in_waiting_rock_value_state(c));

    processCommandAndResetClient(c);

    if (c->querybuf && sdslen(c->querybuf) > 0) 
        processInputBuffer(c);
}

/* Called in main thread.
 * The caller guaranteees not in lock mode. 
 * When some rock keys are recovered,
 * the clients (by joininng together) waiting for the rock keys will be checked 
 * to decide whether they will be resumed. 
 *
 * NOTE1: client may be invalid because the client id 
 *        won't be deleted in read_rock_key_candidates 
 *        while the client has closed a Redis connection.
 *
 * NOTE2: client id may be duplicated in client_ids 
 *        (for case of multi keys recovered).
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
            if (c->rock_key_num == 0)
                resume_command_for_client_in_async_mode(c);
        }
    }
}

/* Called in main thread.
 * If the recover redis key has rock val, recover it.
 * Otherwise do nothing because the key may be deleted or regenerated
 */
static void try_recover_val_object_in_redis_db(const int dbid, 
                                               const char *redis_key, const size_t redis_key_len,
                                               const sds recover_val)
{
    if (recover_val == NULL)
        serverPanic("try_recover_val_object_in_redis_db() the recover_val is NULL(not found) for redis key = %s, dbid = %d", 
                    redis_key, dbid);

    sds key = sdsnewlen(redis_key, redis_key_len);
    redisDb *db = server.db + dbid;
    dictEntry *de = dictFind(db->dict, key);
    if (de != NULL)
    {
        robj *o = dictGetVal(de);
        if (is_rock_value(o))
        {
            #if defined RED_ROCK_DEBUG
            serverAssert(debug_check_type(recover_val, o));
            #endif      
            dictGetVal(de) = unmarshal_object(recover_val);    
        }
    }
    sdsfree(key);
}

/* Called in main thread to recover one key for redis db
 * The caller guarantee in lcok mode.
 *
 * It will try to recover the obj in DB if it can.
 * 
 * The client list will append to waiting_clients 
 * and will be set NULL in read_rock_key_candidates
 */
static void recover_data_for_db(const sds task,
                                const sds recover_val,
                                list **waiting_clients)
{
    int dbid;
    const char *redis_key;
    size_t redis_key_len;
    decode_rock_key_for_db(task, &dbid, &redis_key, &redis_key_len);

    try_recover_val_object_in_redis_db(dbid, redis_key, redis_key_len, recover_val);

    dictEntry *de = dictFind(read_rock_key_candidates, task);
    serverAssert(de && dictGetKey(de) == task);

    list *current = dictGetVal(de);
    serverAssert(current && listLength(current) > 0);
    listJoin(*waiting_clients, current);
    listRelease(current);   // the current right now is empty

    // avoid clear the list of client ids in read_rock_key_candidates when dictDelete
    dictGetVal(de) = NULL;      
}

/* Called in main thread to recover the data from RocksDB.
 * For every finished task, we try to recover the val in Redis DB.
 * And we need join all waiting client ids for all finished tasks.
 * Then we set task array to NULL, delete the finished tasks in candidates,
 *      reclaim all resouce and try to assign new tasks.
 * For the joining waiting clients, check and resume them.
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
            break;

        if (is_rock_key_for_db(task))
        {
            recover_data_for_db(task, read_return_vals[i], &waiting_clients);
        }
        else
        {
            serverAssert(task[0] == ROCK_KEY_FOR_HASH);
            serverPanic("TODO with hash");
        }
        
        // must set NULL for next batch task assignment
        read_key_tasks[i] = NULL;
        sdsfree(read_return_vals[i]);
        read_return_vals[i] = NULL;
        dictDelete(read_rock_key_candidates, task);
    }
    try_assign_tasks();
    rock_r_unlock();

    // NOTE: not in lock mode to call check_client_resume_after_recover_data()
    check_client_resume_after_recover_data(waiting_clients);    

    listRelease(waiting_clients);
}

/* Main thread response the pipe signal which indicates the tasks are finished */
static void on_recover_data(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) 
{
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    UNUSED(fd);

    // clear pipe signal
    char tmp_use_buf[1];
    size_t n = read(rock_pipe_read, tmp_use_buf, 1);     
    serverAssert(n == 1);

    recover_data();
}

/* Called in main thread.
 * After the check for ring buffer, 
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

/* If some key already in ring buf, recover them in Redis DB.
 * Return a list for un-recover keys (may be empty if all redis_keys in ring buffer)
 *
 * NOTE: the un-rocover key is same as the one from the input argument of redis_keys.
 * 
 * If no key in ring buf, return NULL. 
 *
 * The caller guarantee not in lock mode.
 *
 * NOTE: same key could repeat in redis_keys.
 */
static list* check_ring_buf_first_and_recover_for_db(const int dbid, const list *redis_keys)
{
    // Call the API for ring buf in rock_write.c
    list *vals = get_vals_from_write_ring_buf_first(dbid, redis_keys);
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
            // need to recover data which is duplicated from ring buffer
            // NOTE: no need to deal with rock_key_num in client. The caller will take care
            dictEntry *de = dictFind(db->dict, redis_key);
            serverAssert(de);
            if (is_rock_value(dictGetVal(de)))      // NOTE: the same key could repeate for ring buf recovering
                dictGetVal(de) = unmarshal_object(recover_val);     // revocer in redis db
        }
    }

    // reclaim the resource of vals which are allocated 
    // in get_vals_from_write_ring_buf_first()
    listSetFreeMethod(vals, (void (*)(void*))sdsfree);
    listRelease(vals);

    serverAssert(listLength(left) < listLength(redis_keys));
    return left;
}

static void debug_check_all_value_is_rock_value(const int dbid, const list *redis_keys)
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

/* Called in main thread when a client finds it needs some redis keys to
 * continue for a command.
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
 * Otherwise, etrun 1 indicating no need for async and
 * the caller can execute the command right now (sync mode).
 * 
 * The caller needs to reclaim the list after that (which is created by the caller)
 */
int on_client_need_rock_keys_for_db(client *c, const list *redis_keys)
{
    serverAssert(redis_keys && listLength(redis_keys) > 0);

    #if defined RED_ROCK_DEBUG
    debug_check_all_value_is_rock_value(c->db->id, redis_keys);
    #endif

    serverAssert(c->rock_key_num == 0);
    
    const int dbid = c->db->id;
    const uint64_t client_id = c->id;

    int sync_mode = 0;
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
        sync_mode = 1;
        // keep c->rock_key_num == 0 in sync mode
    } 
    else 
    {
        c->rock_key_num = listLength(left);
        go_on_need_rock_keys_from_rocksdb(client_id, dbid, left);
    }

    if (left)
        // The left has the same key in redis_keys, so only release the list
        listRelease(left);      

    return sync_mode;
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

/* API for rock_write.c for checking whether the key is in candidates
 * Called in main thread.
 * Return 1 if it is in read_rock_key_candidates. Otherwise 0.
 */
int already_in_candidates_for_db(const int dbid, const sds redis_key)
{
    int exist = 0;
    sds rock_key = sdsdup(redis_key);
    rock_key = encode_rock_key_for_db(dbid, rock_key);

    rock_r_lock();
    if (dictFind(read_rock_key_candidates, rock_key) != NULL)
        exist = 1;
    rock_r_unlock();

    sdsfree(rock_key);

    return exist;
}

/* the API for start the read thread */
void init_and_start_rock_read_thread()
{
    init_rock_read();

    if (pthread_create(&rock_read_thread_id, NULL, rock_read_main, NULL) != 0) 
        serverPanic("Unable to create a rock read thread.");
}
