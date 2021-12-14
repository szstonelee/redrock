#include "rock_read.h"
#include "rock.h"
#include "server.h"
#include "sds.h"
#include "rock_marshal.h"

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


/*
 * The critical data is a hash table and array of tasks (task key and return value)
 *
 * 1. For hash table, i.e., read_rock_key_candidates,
 *    key is the rock_key and value is a list of clients(client_id) waiting for the key
 *    NOTE: list may be NULL before the key is deleted from read_rock_key_candidates
 *          when return task needs to join all lists 
 * 
 * 2. For array of tasks, i.e., read_key_tasks and read_return_vals and task_status,
 *    read_key_tasks is the tasks for the read thread. (from the beginning until NULL)
 * 
 * NOTE1: read thread needs to copy tasks to avoid data race when read from RocksDB.
 *
 * NOTE2: Task is the rock key to read. 
 *        It points to the hash table key (shared with read_rock_key_candidates)
 *        So if a key is removed from read_rock_key_candidates, 
 *        it is needed to remove from the array first by setting to NULL.
 * 
 * When main thread finishes assigning tasks, it sets task_status to READ_START_TASK
 *      and the read thread will loop to check task_status and can start to work.
 *      NOTE: if zero task, keep the task_status to READ_RETURN_TASK.
 * 
 * When read thread finishes reading from RocksDB, 
 *      it sets task_status to READ_RETURN_TASK
 *      and signals the main thread to recover data
 * 
 * When main thread recover data, it will check whether it can recover 
 *      because the key may be deleted, modified by other clients f
 *      rom Redis db by other clients.
 *      Reover condition is that the val is still of rock key val.
 *      Main thread guarantees not evict any key which has been already in candidates
 *      to avoid recover the not-match key (because key could've be regenerated)
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
static sds read_key_tasks[READ_TOTAL_LEN] __attribute__((aligned(64))); // frient to cpu cache line
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

/* the API for start the read thread */
static void* rock_read_main(void *arg);
void init_and_start_rock_read_thread()
{
    init_rock_read();

    pthread_t read_thread;
    if (pthread_create(&read_thread, NULL, rock_read_main, NULL) != 0) 
        serverPanic("Unable to create a rock read thread.");
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
            break;

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
            vals[i] = NULL;     // not found
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
 * Returning 0 means no need to work, 
 * indicating the read thread needs to hava a sleep for a while
 * Otherwise return 1 indicating the tasks has been done,
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

    unsigned int sleep_us = MIN_SLEEP_MICRO;
    while(1)
    {
        if (do_tasks() != 0)
        {
            sleep_us = MIN_SLEEP_MICRO;     // if we have task, shorten the sleep time
            continue;       // no sleep, go on for more task
        }        

        usleep(sleep_us);
        sleep_us <<= 1;        // double sleep time
        if (sleep_us > MAX_SLEEP_MICRO) 
            sleep_us = MAX_SLEEP_MICRO;
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

/* Called in main thread to add tasks.
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
 * Return 1 if successfully assigning task. Otherwise return 0.
 */
static int try_assign_tasks()
{
    if (task_status == READ_START_TASK)
        return 0;   // read thread is working, can not assign task

    sds tasks[READ_TOTAL_LEN];
    const int avail = get_keys_from_candidates_before_assignment(tasks);
    if (avail != 0)
        assign_tasks(avail, tasks);    // change task_status to READ_START_TASK
    // else{}, need to keep task_status to READ_RETURN_TASK

    return avail != 0 ? 1 : 0;
}

/* Called by main thraed because c->rock_key_num changes to zero.
 * The caller guaratee noot in lock mode.
 */
static void resume_command_for_client(client *c)
{
    UNUSED(c);      // TODO
}

/* Called in main thread.
 * The caller guaranteee not in lock mode.
 * When some rock keys are recovered,
 * the clients (by joininng together) waiting for the rock keys will be checked 
 * to decide whether they will be resumed for the command. 
 *
 * NOTE1: client may be invalid because the client id 
 *        won't be deleted in read_rock_key_candidates 
 *        when client close a Redis connection.
 *
 * NOTE2: client id may be duplicated in client_ids 
 *        (for case of multi key recovered).
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
                resume_command_for_client(c);
        }
    }
}

/* If the recover redis key has rock val, recover it.
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

/* Called in main thread to recover one key, i.e., rock_key.
 * The caller guarantees lock mode, 
 * so be careful of no reentry of the lock.
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
    listJoin(waiting_clients, current);
    serverAssert(listLength(current) == 0);
    listRelease(current);
}

/* Called in main thread to recover the data from RocksDB 
 * when received the signal from read thread.
 */
static void recover_data()
{
    list *waiting_clients = listCreate();

    rock_r_lock();
    serverAssert(task_status == READ_RETURN_TASK);
    serverAssert(read_key_tasks[0] != NULL);
    for (int i = 0; i < READ_TOTAL_LEN; ++i)
    {
        sds task = read_key_tasks[i];
        if (task == NULL)
            break;
        
        // join this client id for the task to waiting_clients
        recover_one_key(task, read_return_vals[i], waiting_clients);
        
        read_key_tasks[i] = NULL;
        sdsfree(read_return_vals[i]);
        read_return_vals[i] = NULL;
        // task resource will be reclaimed (the list is NULL right now) 
        int res = dictDelete(read_rock_key_candidates, task);
        serverAssert(res == DICT_OK);
    }
    try_assign_tasks();
    rock_r_unlock();

    // NOTE: not in lock mode
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

static int debug_check_no_exist_client_id(const list *l, const uint64_t id)
{
    listNode *exist = listSearchKey((list*)l, (void*)id);
    return exist == NULL ? 1 : 0;
}

/* Called in main thread when a client finds it needs some redis keys to
 * continue for a command.
 * The caller guarantee not use read lock.
 * The redis_keys is the list of keys needed by the command (i.e., with rock value)
 * The caller needs to reclaim the list after that (which is created by the caller)
 */
void on_client_need_rock_keys(const client *c, const list *redis_keys)
{
    serverAssert(redis_keys && listLength(redis_keys) > 0);

    const int dbid = c->db->id;
    const uint64_t client_id = c->id;

    listIter li;
    listNode *ln;
    listRewind((list*)redis_keys, &li);

    rock_r_lock();

    while ((ln = listNext(&li)))
    {
        sds redis_key = listNodeValue(ln);
        sds rock_key = sdsdup(redis_key);
        rock_key = encode_rock_key(dbid, rock_key);

        dictEntry *de = dictFind(read_rock_key_candidates, rock_key);
        if (de == NULL)
        {
            list *client_ids = listCreate();
            listAddNodeHead(client_ids, (void*)client_id);  
            // transfer ownership of rock_key and client_ids to read_rock_key_candidates
            dictAdd(read_rock_key_candidates, rock_key, client_ids);    
        }
        else
        {
            list *client_ids = dictGetVal(de);
            #if defined RED_ROCK_DEBUG
            serverAssert(debug_check_no_exist_client_id(client_ids, client_id));
            #endif
            listAddNodeTail(client_ids, (void*)client_id);
            sdsfree(rock_key);
        }
    }

    try_assign_tasks();

    rock_r_unlock();
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
    rock_r_lock();
    for (int i = 0; i < len; ++i)
    {
        if (dictFind(read_rock_key_candidates, rock_keys[i]))
        {            
            rock_r_unlock();
            return 0;
        }
    }
    rock_r_unlock();

    return 1;
}

/* API for rock_write.c for checking whether the key is in candidates
 * Called in main thread.
 * Return 1 if it is in read_rock_key_candidates. Otherwise 0.
 */
int already_in_candidates(const int dbid, const sds redis_key)
{
    int exist = 0;
    sds rock_key = sdsdup(redis_key);
    rock_key = encode_rock_key(dbid, rock_key);
    rock_r_lock();
    if (dictFind(read_rock_key_candidates, rock_key) != NULL)
        exist = 1;
    rock_r_unlock();
    sdsfree(rock_key);
    return exist;
}