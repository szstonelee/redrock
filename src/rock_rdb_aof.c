#include "rock_rdb_aof.h"
#include "rock.h"
#include "rock_write.h"
#include "rock_marshal.h"

#include <unistd.h>

// If in main redis proocess, it is zero. otherwise, it is the child process id
static int child_process_id = 0;

// Pipe for child process and service thread in redis process for rdb or aof
static int pipe_request[2];
static int pipe_response[2];

static int service_thread_is_running;
static pthread_t service_thread;

static pthread_mutex_t mutex;       // safe guard pipe between main thread and service thread in redis process
static pthread_mutex_t mutex_child_process;     // if unlocked, indicating child process has been started

static const rocksdb_snapshot_t *snapshot = NULL;    // RocksDB snapshot for rdb/aof service thread to read data

/* Called when redis start in main thread */
void init_for_rdb_aof_service()
{
    serverAssert(pthread_mutex_init(&mutex, NULL) == 0);
    serverAssert(pthread_mutex_init(&mutex_child_process, NULL) == 0);

    serverAssert(pthread_mutex_lock(&mutex) == 0);
    pipe_request[0] = -1;
    pipe_request[1] = -1;
    pipe_response[0] = -1;
    pipe_response[1] = -1;
    serverAssert(pthread_mutex_unlock(&mutex) == 0);

    // NOTE: only first time init to zero, in future, we use
    //       pthread_tryjoin_np() to determine whether the service thread is running
    service_thread_is_running = 0;       
}

/* After service thread started succesfully,
 * it needs to close the unecesssary file descriptor of pipe.
 * including:
 * 1. request write end
 * 2. response read end
 * 
 * NOTE: it must guarantee that child process has already started
 *       because here will modify the pipe file descriptor (set to -1)
 */
static void close_not_used_pipe_in_service_thread()
{
    serverAssert(pthread_mutex_lock(&mutex) == 0); 

    serverAssert(pipe_request[1] != -1);
    close(pipe_request[1]);
    pipe_request[1] = -1;

    serverAssert(pipe_response[0] != -1);
    close(pipe_response[0]);
    pipe_response[0] = -1;

    serverAssert(pthread_mutex_unlock(&mutex) == 0);
}

/* When child process start, it needs to 
 * close the unecessary file descriptor of pipe.
 * including:
 * 1. request read end
 * 2. response write end
 */
void close_unused_pipe_in_child_process_when_start()
{
    serverAssert(pipe_request[0] != -1 && pipe_request[1] != -1);
    serverAssert(pipe_response[0] != -1 && pipe_response[1] != -1);

    close(pipe_request[0]);
    pipe_request[0] = -1;

    close(pipe_response[1]);
    pipe_response[1] = -1;
}

/* Three cases will call here to close the pipe 
 * (if pipe is opened for current processs. 
 * NOTE: child process will copy the file descriptor table (i.e., opened in child process by default)
 * 
 * 1. The child process exit
 * 
 * 2. The service thread in redis thread exit
 * 
 * 3. The failure of starting a service thread from main thread
 * 
 * For 2 and 3, we must use lock.
 */
static void clear_all_resource_of_pipe(const int from_redis_process)
{
    if (from_redis_process)
        serverAssert(pthread_mutex_lock(&mutex) == 0);

    if (pipe_request[0] != -1)
    {
        close(pipe_request[0]);
        pipe_request[0] = -1;
    }
    
    if (pipe_request[1] != -1)
    {
        close(pipe_request[1]);
        pipe_request[1] = -1;
    }

    if (pipe_response[0] != -1)
    {
        close(pipe_response[0]);
        pipe_response[0] = -1;
    }

    if (pipe_response[1] != -1)
    {
        close(pipe_response[1]); 
        pipe_response[1] = -1;
    }

    if (from_redis_process)
        serverAssert(pthread_mutex_unlock(&mutex) == 0);
}

/* Called in main thread to create a pipe before starting a service thread.
 *
 * If sucess, return 1. Otherwise, return 0 with resource cleanup.
 */
static int create_pipe()
{
    serverAssert(pthread_mutex_lock(&mutex) == 0);
    
    serverAssert(pipe_request[0] == -1 && pipe_request[1] == -1);
    serverAssert(pipe_response[0] == -1 && pipe_request[1] == -1);

    if (pipe(pipe_request) != 0) 
    {
        serverLog(LL_WARNING, "create_pipe() piep_request for rdb/aof faiiled1");
        serverAssert(pthread_mutex_unlock(&mutex) == 0);
        return 0;        
    }

    if (pipe(pipe_response) != 0)
    {
        close(pipe_request[0]);
        close(pipe_request[1]);
        serverAssert(pthread_mutex_unlock(&mutex) == 0);
        return 0;
    }

    serverAssert(pthread_mutex_unlock(&mutex) == 0);
    return 1;
}

/* Called in main thread to start a service thread
 * return 1 if success, otherwisse return 0.
 */
static void* service_thread_for_rdb_aof_main(void *arg);
static int start_service_thread()
{
    serverAssert(pthread_mutex_lock(&mutex_child_process) == 0);    // let service thread wait
    if (pthread_create(&service_thread, NULL, service_thread_for_rdb_aof_main, NULL) != 0)
    {
        serverLog(LL_WARNING, "service thread can not start (1)!");
        serverAssert(pthread_mutex_unlock(&mutex_child_process) == 0);
        clear_all_resource_of_pipe(1);
        return 0;
    }

    service_thread_is_running = 1;
    return 1;
}

/* Called in redis process when a rdb sub process will launch.
 * return 1 to tell the caller that the child process can start.
 * return 0 to tell the caller it can not start for the following reason.
 * 
 * Failure including:
 * 1. the old service thread is busy and has not exited
 * 2. pipe can not be inialized
 * 3. service thread can not start
 */
int on_start_rdb_aof_process()
{
    serverAssert(child_process_id == 0);

    if (!service_thread_is_running)
    {
        if (!create_pipe())
            return 0;
                
        return start_service_thread();
    }
    else
    {
        // we need check whether the service thread is actually running
        const int ret = pthread_tryjoin_np(service_thread, NULL);  // non-blocking call
        if (ret == 0)
        {
            // service thread has exited
            service_thread_is_running = 0;
        }
        else
        {
            serverAssert(ret == EBUSY);
            return 0;       // service thread is busy
        }

        if (!create_pipe())
            return 0;
        
        return start_service_thread();
    }
}

/* Called in main thread when it guarantees that the child process has been runninng
 * So the service thread can modify the pipe (by close unused pipe)
 * 
 *  NOTE: because child process must have all available pipe (not -1)
 */
void signal_child_process_already_running()
{
    serverAssert(pthread_mutex_unlock(&mutex_child_process) == 0);
}

/* Called in child process when child process just started.
 * 
 * So we can distinguish which is the redis process.
 * If child_process_id == 0, it is the redis process.
 * Otherwise, it is a child process (for rdb/aof).
 */
void set_process_id_in_child_process_for_rock()
{
    serverAssert(child_process_id == 0);

    child_process_id = getpid();

    serverLog(LL_WARNING, "child process id = %d", child_process_id);
}

/* Called in child process before exit()
 */
void on_exit_rdb_aof_process()
{
    serverAssert(child_process_id != 0);

    clear_all_resource_of_pipe(0);     // called in child process, so not use lock
}

static robj* direct_read_one_key_val_from_rocksdb(const int dbid, const sds key)
{
    sds rock_key = sdsdup(key);
    rock_key = encode_rock_key_for_db(dbid, rock_key);

    size_t db_val_len;
    char *err = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    char *db_val = rocksdb_get(rockdb, readoptions, rock_key, sdslen(rock_key), &db_val_len, &err);
    rocksdb_readoptions_destroy(readoptions);
    
    if (err)
        serverPanic("direct_read_one_key_val_from_rocksdb(), err = %s, key = %s", err, key);
    
    if (db_val == NULL)
        // NOT FOUND, but it is illegal
        serverPanic("direct_read_one_key_val_from_rocksdb() not found for key = %s", key);
    
    sds v = sdsnewlen(db_val, db_val_len);
    robj *o = unmarshal_object(v);

    // reclaim resource
    rocksdb_free(db_val);
    sdsfree(v);
    sdsfree(rock_key);

    return o;
}

static sds direct_read_one_field_val_from_rocksdb(const int dbid, const sds hash_key, const sds field)
{
    sds rock_key = sdsdup(hash_key);
    rock_key = encode_rock_key_for_hash(dbid, rock_key, field);

    size_t db_val_len;
    char *err = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    char *db_val = rocksdb_get(rockdb, readoptions, rock_key, sdslen(rock_key), &db_val_len, &err);
    rocksdb_readoptions_destroy(readoptions);

    if (err)
        serverPanic("direct_read_one_field_val_from_rocksdb(), err = %ss, key = %s, field =%s", err, hash_key, field);
    
    if (db_val == NULL)
        // NOT FOUND, but it is illegal
        serverPanic("direct_read_one_field_val_from_rocksdb not found for key = %s, field = %s", hash_key, field);

    const sds v = sdsnewlen(db_val, db_val_len);

    // reclaim resource
    rocksdb_free(db_val);
    sdsfree(rock_key);

    return v;
}

static robj* read_from_disk_for_key_in_redis_process(const int dbid, const sds key)
{
    sds val = get_key_val_str_from_write_ring_buf_first_in_redis_process(dbid, key);
    if (val != NULL)
    {
        robj *o = unmarshal_object(val);
        sdsfree(val);
        return o;
    }
    else
    {
        // need read from RocksDB
        return direct_read_one_key_val_from_rocksdb(dbid, key);
    }
}

/* For hash_key, some fields are in disk, we need create an object value for the whole key  */
static robj* read_from_disk_for_whole_key_of_hash_in_redis_process(const int dbid, const sds hash_key)
{
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, hash_key);
    serverAssert(de_db);
    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);
    dict *hash = o->ptr;
    const size_t hash_cnt = dictSize(hash);

    dict *new_hash = dictCreate(&hashDictType, NULL);
    if (hash_cnt > DICT_HT_INITIAL_SIZE)
        dictExpand(new_hash, hash_cnt);

    dictIterator *di = dictGetIterator(hash);
    dictEntry *de;
    size_t val_in_rock_cnt = 0;
    while ((de = dictNext(di)))
    {
        const sds field = dictGetKey(de);
        const sds val = dictGetVal(de);

        const sds copy_field = sdsdup(field);

        if (val == shared.hash_rock_val_for_field)
        {
            sds v = get_field_val_str_from_write_ring_buf_first_in_redis_process(dbid, hash_key, field);
            if (v != NULL)
            {
                dictAdd(new_hash, copy_field, v);
            }
            else
            {
                v = direct_read_one_field_val_from_rocksdb(dbid, hash_key, field);
                serverAssert(v != NULL);
                dictAdd(new_hash, copy_field, v);
            }
            ++val_in_rock_cnt;
        }
        else
        {
            const sds copy_val = sdsdup(val);
            dictAdd(new_hash, copy_field, copy_val);
        }
    }
    dictReleaseIterator(di);

    serverAssert(val_in_rock_cnt > 0);
    robj *new_o = createObject(OBJ_HASH, new_hash);
    new_o->encoding = OBJ_ENCODING_HT;
    return new_o;
}

static robj* read_from_disk_in_redis_process(const int have_field_in_disk, const int dbid, const sds key)
{
    if (have_field_in_disk)
    {
        return read_from_disk_for_whole_key_of_hash_in_redis_process(dbid, key);
    }
    else
    {
        return read_from_disk_for_key_in_redis_process(dbid, key);
    }
}

static robj* read_from_disk_in_child_process(const int have_field_in_disk, const int dbid, const sds key)
{
    UNUSED(have_field_in_disk);
    UNUSED(dbid);
    UNUSED(key);

    return NULL;
}

/* Check whether the value needs to get from RocksDB if the o is in RocksDB.
 * If o is not roock value or not in rock_hash, 
 *    we return the input o, 
 * otherwise, we return the value from disk.
 *
 * It can be in main process of Redis or child process for rdb or aof.
 */
robj* get_value_if_exist_in_rock_for_rdb_afo(const robj *o, const int dbid, const sds key)
{
    redisDb *db = server.db + dbid;

    int have_field_in_disk = 0;
    if (!is_rock_value(o))
    {
        dictEntry *de = dictFind(db->rock_hash, key);
        if (de == NULL)
        {
            return (robj*)o;   // not rock value and not exist in rock hash, it is a real value
        }
        else
        {
            serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);
            dict *lrus = dictGetVal(de);
            dict *hash = o->ptr;

            if (dictSize(hash) == dictSize(lrus))
            {
                return (robj*)o;       // all fields not in disk, it is a real value
            }
            else
            {
                have_field_in_disk = 1;
            }
        }
    }

    // We need create a temporary o_disk and return it
    // The caller has the responsibility to releasse o_disks
    robj *o_disk = NULL;
    if (child_process_id == 0)
    {
        o_disk = read_from_disk_in_redis_process(have_field_in_disk, dbid, key);
    }
    else
    {
        o_disk = read_from_disk_in_child_process(have_field_in_disk, dbid, key);
    }

    serverAssert(o_disk != NULL);
    return o_disk;
}

/* The main entrance for rdb/aof service thread
 */
static void* service_thread_for_rdb_aof_main(void *arg)
{
    UNUSED(arg);

    // waiting the child process to start (by giving a signal from main thread)
    // so child process copy all the valid pipe
    // because before the service thread start, mutex_child_process will be locked
    // and after the child proocess has started, the main thread will unlock.

    // NOTE: pthread_mutex_lock could return an error 
    //       if service thread starts so so so slow which is later than
    //       the main thead gets the signal (because child process fork() finished) 
    //       and unlcok the mutex_child_process.
    //       It is very rare, but we can ignore and go on without any problem.
    if (pthread_mutex_lock(&mutex_child_process) == 0)   
        pthread_mutex_unlock(&mutex_child_process);

    // then we can modify pipe
    close_not_used_pipe_in_service_thread();

    serverAssert(snapshot == NULL);
    snapshot = rocksdb_create_snapshot(rockdb);
    serverAssert(snapshot != NULL);
    serverLog(LL_NOTICE, "we start a rdb/aof thread service with read-only snapshot for child process!");

    serverLog(LL_WARNING, "service thread sleep for 20 seconds");
    sleep(20);
    serverLog(LL_WARNING, "service thread wakeup");

    rocksdb_release_snapshot(rockdb, snapshot);
    snapshot = NULL;
    clear_all_resource_of_pipe(1);

    return NULL;
}