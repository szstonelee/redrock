/* NOTE: if not use gcc ignored warning for -Warray-bounds
 * service_thread_main() statement: request_len = *((size_t*)len_buf);
 * will genearte warning (in sds.h)
 * 
 * the warning is like: 
 * sds.h:88:28: warning: array subscript -1 is outside array bounds of ‘char[30]’ [-Warray-bounds]
 * 
 * I tried a lot of methods but it does not work.
 * It is wiered that receive_response_in_child_process() use similiar code
 * but does not generate warning.
 * And before add receive_response_in_child_process() no warning.
 * And the warning only for -O2 and above, I compile -O0, it is OK.
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#include "rock_rdb_aof.h"
#pragma GCC diagnostic pop

#include "rock.h"
#include "rock_write.h"
#include "rock_marshal.h"

#include <unistd.h>
#include <pthread.h>

// If in main redis proocess, it is zero. otherwise, it is the child process id
// it is only accessed by main thread of redis process and child process
static int child_process_id = 0;

// Pipe for child process and service thread in redis process for rdb or aof
// They are accessed by main thread of redis process and main thread of child process
// and also include service thread.
// So the logic and mutex_main_and_service need to guarantee the visibility of those threads.
// For redis process, pipe creatation is inited in main thread before service thread start
//                    and the close of unused pipe is after the mutex_main_and_service in service thread.
static int pipe_request[2];
static int pipe_response[2];

// NOTE: service_thread is only accessed by main thread
static pthread_t *service_thread = NULL;         

// safe guard pipe between main thread and service thread in redis process
// if unlocked, indicating child process has been started
// and it is also used for sync for main thread and service thread
//     for 1. pipe, 2. snapshot of ring buffer and RocksDB
static pthread_mutex_t mutex_main_and_service;     

// NOTE: the following three variables are accessed by main thread and service thread
//       They are guarded by mutex_main_and_service
// RocksDB snapshot for rdb/aof service thread to read data
static const rocksdb_snapshot_t *snapshot = NULL;  
// ring buffer snapshot for rdb/aof servive thread to read data  
static sds child_ringbuf_keys[RING_BUFFER_LEN];
static sds child_ringbuf_vals[RING_BUFFER_LEN];

// it is only used in servie thread
static sds request_for_service_thread = NULL;


static redisAtomic int cancel_service_thread;

/* Called when redis start in main thread 
 * Here we use lock for sync.
 */
void init_for_rdb_aof_service()
{
    atomicSet(cancel_service_thread, 0);

    serverAssert(pthread_mutex_init(&mutex_main_and_service, NULL) == 0);

    serverAssert(pthread_mutex_lock(&mutex_main_and_service) == 0);

    pipe_request[0] = -1;
    pipe_request[1] = -1;
    pipe_response[0] = -1;
    pipe_response[1] = -1;

    // NOTE: only first time init to zero, in future, we use
    //       pthread_tryjoin_np() to determine whether the service thread is running

    // init child_ring_buf copy
    for (int i = 0; i < RING_BUFFER_LEN; ++i)
    {
        child_ringbuf_keys[i] = NULL;
        child_ringbuf_vals[i] = NULL;
    }       

    serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
}

/* After service thread started succesfully,
 * it needs to close the unecesssary file descriptor of pipe.
 * including:
 * 1. request write end
 * 2. response read end
 * 
 * NOTE1: it must guarantee that child process has already started
 *        because here will modify the pipe file descriptor (set to -1)
 * 
 * NOTE2: the caller must guarantee in lock mode.
 */
static void close_not_used_pipe_in_service_thread()
{
    serverAssert(pipe_request[1] != -1);
    close(pipe_request[1]);
    pipe_request[1] = -1;

    serverAssert(pipe_response[0] != -1);
    close(pipe_response[0]);
    pipe_response[0] = -1;
}

/* When child process start, it needs to 
 * close the unecessary file descriptor of pipe.
 * including:
 * 1. request read end
 * 2. response write end
 * 
 * NOTE: child process can not use lock which is only for redis process
 */
static void close_unused_pipe_in_child_process_when_start()
{
    serverAssert(pipe_request[0] != -1 && pipe_request[1] != -1);
    serverAssert(pipe_response[0] != -1 && pipe_response[1] != -1);

    close(pipe_request[0]);
    pipe_request[0] = -1;

    close(pipe_response[1]);
    pipe_response[1] = -1;    
}

/* Called in child process when child process just started.
 * 
 * So we can distinguish which is the redis process.
 * If child_process_id == 0, it is the redis process.
 * Otherwise, it is a child process.
 */
static void set_process_id_in_child_process_for_rock()
{
    serverAssert(child_process_id == 0);

    child_process_id = getpid();
}

/* Called in child process to send a request for dbid and key.
 * 
 * The encoding of request is like this:
 * The whole buf is composed of one byte of dbid and the key.
 * We need sene size_t header of the size of the buf.
 * Then send the buf.
 * 
 * Return True(1) if sucess, otherwise false(0).
 */
static int send_request_in_child_process(const int dbid, const sds key)
{
    serverAssert(child_process_id != 0);
    serverAssert(dbid >= 0 && dbid < server.dbnum && key);

    const size_t send_sz = 1 + sdslen(key);
    ssize_t write_res;

    // first, the size of buf
    write_res = write(pipe_request[1], &send_sz, sizeof(size_t));
    if (write_res < 0 || (size_t)write_res != sizeof(size_t))
    {
        serverLog(LL_WARNING, "send request header fail in child process, write_res = %zu", write_res);
        return 0;
    }

    // then the dbid
    unsigned char c_dbid = (unsigned char)dbid;
    write_res = write(pipe_request[1], &c_dbid, 1);
    if (write_res != 1)
    {
        serverLog(LL_WARNING, "send request dbid fail in child process, write_res = %zu", write_res);
        return 0;
    }

    // last the key
    write_res = write(pipe_request[1], key, sdslen(key));
    if (write_res < 0 || (size_t)write_res != sdslen(key))
    {
        serverLog(LL_WARNING, "send request key fail in child process, write_res = %zu", write_res);
        return 0;
    }

    return 1;
}

/* Called in child process.
 *
 * After the chiild process send a request, it will wait the response.
 * return NULL if error, otherwise, the 
 */
static sds receive_response_in_child_process()
{
    serverAssert(child_process_id != 0);

    // first read the length of size of size_t
    #define PIPE_READ_BUF_LEN 64
    char buf[PIPE_READ_BUF_LEN];
    
    char len_buf[sizeof(size_t)];
    int len_index = 0;
    ssize_t response_len = -1;

    int error = 0;
    sds response = NULL;

    while (1)
    {
        const ssize_t read_res = read(pipe_response[0], buf, PIPE_READ_BUF_LEN);
        if (read_res < 0)
        {
            serverLog(LL_WARNING, "read error from pipe in child process ");
            error = 1;
            break;
        }
        else if (read_res == 0)
        {
            serverLog(LL_WARNING, "read EOF from pipe in child process");
            error = 1;
            break;
        }

        // We really read something
        char *data = buf;
        size_t data_len = (size_t)read_res;

        if (response_len == -1)
        {
            // first, we need read response_len
            const int more_for_len = sizeof(size_t) - len_index;
            serverAssert(more_for_len > 0 && more_for_len <= (int)sizeof(size_t));

            if (read_res < (ssize_t)more_for_len)
            {
                // not enough for the response_len
                memcpy(len_buf+len_index, buf, read_res);
                len_index += read_res;
            }
            else
            {
                // we can get response_len
                memcpy(len_buf+len_index, buf, more_for_len);
                const size_t real_len = *((size_t*)len_buf);
                serverAssert(real_len <= SSIZE_MAX); 
                response_len = (ssize_t)real_len;

                serverAssert(response == NULL);
                response = sdsempty();
                response = sdsMakeRoomFor(response, (size_t)response_len);

                data += more_for_len;
                data_len -= more_for_len;
            }
        }

        if (response_len == 0)
        {
            serverAssert(data_len == 0);
            break;  // no need to read more
        }
        else if (response_len > 0)
        {
            // read the response real content
            if (data_len > (size_t)response_len || data_len + sdslen(response) > (size_t)response_len)
            {
                error = 1;
                serverLog(LL_WARNING, "read too much data for response in child process, data_len = %zu, response_len = %zu, sds(response) = %zu",
                                      data_len, response_len, sdslen(response));
                break;
            }

            response = sdscatlen(response, data, data_len);
            if (sdslen(response) == (size_t)response_len)
                break;      // finish reading response
        }
    }

    if (error)
    {
        sdsfree(response);
        return NULL;
    }
    else
    {
        serverAssert(response != NULL);
        return response;
    }
}

/* Run in child process.
 * 
 * When the child process is forked, it first call here to init something,
 * like set child process id, close unused pipe.
 */
void on_start_in_child_process()
{
    set_process_id_in_child_process_for_rock();
    close_unused_pipe_in_child_process_when_start();

    // The following is for test
    // sds request = sdsnew("keyisabc");
    const unsigned char dbid = 5;
    sds request_key = sdsempty();
    for (int i = 0; i < 10000000; ++i)
    {
        request_key = sdscat(request_key, "a");
    }
    if (send_request_in_child_process(dbid, request_key))
    {
        // get response
        sds response = receive_response_in_child_process();
        serverLog(LL_WARNING, "on_start_in_child_process() debug, respoonse = %s", response);
        sdsfree(response);
    }
    sdsfree(request_key);
    sleep(5);
}

/* Two cases will call here to close the pipe 
 * (if pipe is opened for current processs. 
 * NOTE: child process will copy the file descriptor table (i.e., opened in child process by default)
 * 
 * 1. The child process exit
 * 
 * 2. The service thread in redis thread exit (normal or not-normal when child process can not start)
 *    For 2, the caller must guarantee in lock.
 */
static void clear_all_resource_of_pipe()
{
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
}

/* Called in main thread to create a pipe after starting a service thread.
 * The caller guarantee in lock mode.
 *
 * If sucess, return true(1). Otherwise, return false(0) with resource cleanup.
 */
static int create_pipe()
{   
    serverAssert(pipe_request[0] == -1 && pipe_request[1] == -1);
    serverAssert(pipe_response[0] == -1 && pipe_request[1] == -1);

    if (pipe(pipe_request) != 0) 
    {
        pipe_request[0] = -1;
        pipe_request[1] = -1;
        serverLog(LL_WARNING, "create_pipe() pipe_request failed");
        return 0;        
    }

    if (pipe(pipe_response) != 0)
    {
        close(pipe_request[0]);
        close(pipe_request[1]);
        pipe_request[0] = -1;
        pipe_request[1] = -1;
        pipe_response[0] = -1;
        pipe_response[1] = -1;
        serverLog(LL_WARNING, "create_pipe() pipe_response failed");
        return 0;
    }

    return 1;
}

/* Called in service thread before exit.
 * The caller must guarantee in lock mode.
 */
static void clear_snapshot_of_ring_buffer()
{
    for (int i = 0; i <RING_BUFFER_LEN; ++i)
    {
        if (child_ringbuf_keys[i] != NULL)
        {
            serverAssert(child_ringbuf_vals[i] != NULL);

            sdsfree(child_ringbuf_keys[i]);
            sdsfree(child_ringbuf_keys[i]);

            child_ringbuf_keys[i] = NULL;
            child_ringbuf_vals[i] = NULL;
        }
    }
}

/* Called in main thread of redis process to start a service thread
 * return true(1) if success, otherwisse return false(0).
 */
static void* service_thread_main(void *arg);
static int start_service_thread()
{
    atomicSet(cancel_service_thread, 0);

    if (pthread_create(service_thread, NULL, service_thread_main, NULL) != 0)
    {
        serverLog(LL_WARNING, "service thread can not start (1)!");
        return 0;
    }

    return 1;
}

/* Called in main thread when
 * 1. pipe can not create
 * 2. child process can not start
 * 
 * The caller guarantee in lock.
 */
static void terminate_service_thread(const char *reason)
{
    serverAssert(snapshot == NULL);
    serverAssert(child_ringbuf_keys[0] == NULL);

    atomicSet(cancel_service_thread, 1);        // signal service thread to abort

    serverAssert(pthread_join(*service_thread, NULL) == 0);

    clear_all_resource_of_pipe();

    serverLog(LL_WARNING, "We need terminate the service thread for RDB/AOF bgsave because reason = %s.", reason);
}

/* Called in main thread of redis process before a rdb child process launch.
 * return 1 to tell the caller that the child process can start.
 * return 0 to tell the caller it can not start a child processs for the following reasons.
 * 
 * Reason for not start a child process including:
 * 1. the old service thread is busy and has not exited
 * 2. pipe can not be inialized
 * 3. service thread can not start
 */
int on_start_rdb_aof_process()
{
    serverAssert(child_process_id == 0);

    if (service_thread == NULL)
    {
        service_thread = zmalloc(sizeof(*service_thread));

        serverAssert(pthread_mutex_lock(&mutex_main_and_service) == 0);    // let service thread wait        
        if (!start_service_thread())
        {
            serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
            return 0;
        }

        if (!create_pipe())
        {
            terminate_service_thread("can not create pipe");
            serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
            return 0;
        }

        // NOTE: mutex_main_and_service will be locked 
        // until signal_child_process_already_running() is called
        // which guarantee that service thread work after the child process have been forked()

        return 1;
    }
    else
    {
        // we need check whether the service thread is actually running
        const int check = pthread_tryjoin_np(*service_thread, NULL);  // non-blocking call
        if (check != 0)
        {
            serverAssert(check == EBUSY);
            return 0;       // service thread is busy
        }

        // service thread has exited

        serverAssert(pthread_mutex_lock(&mutex_main_and_service) == 0);    // let service thread wait
        if (!start_service_thread())
        {
            serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
            return 0;
        }

        if (!create_pipe())
        {
            terminate_service_thread("can not create pipe");
            serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
            return 0;
        }
        
        // NOTE: mutex_main_and_servicej will be locked for a while
        // until signal_child_process_already_running() is called
        // which guarantee that service thread work after the child process have been forked()

        return 1;
    }
}

/* Called in main thread when it guarantees that the child process has been runninng
 * or completely failed (i.e., child_pid == -1)
 *
 * If child_pid == -1, we need terminate the service thread and release resource
 * like pipe and the lock.
 * 
 * If the child process can start, and we unlcok the mutex_main_and_service
 * to make the service thread to work, 
 * e.g., can modify the pipe (by close unused pipe which can not be closed before child process).
 *       NOTE: because child process must have all available pipe (not -1)
 * 
 * And we also create snapshot for ring buffer and RocksDB before service thread start to work.
 * 
 * NOTE: snapshot creatation must be called in main thread
 *       because it guarantees the dataset of redis (all keys and values) 
 *       at the call time is immutable.
 */
void signal_child_process_already_running(const int child_pid)
{
    if (child_pid == -1)
    {
        terminate_service_thread("child process can not start");
        serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
        return;
    }

    // creatation of snapshot
    // NOTE: we need call ring buffer first, because write thread will 
    //       change ring buffer, we must guarantee no data loss for snapshot
    serverAssert(snapshot == NULL);
    serverAssert(child_ringbuf_keys[0] == NULL);
    create_snapshot_of_ring_buf_for_child_process(child_ringbuf_keys, child_ringbuf_vals);
    snapshot = rocksdb_create_snapshot(rockdb);
    serverAssert(snapshot != NULL);

    // signal service thread to work
    serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
}

/* Called in child process before exit()
 */
void on_exit_rdb_aof_process()
{
    serverAssert(child_process_id != 0);

    clear_all_resource_of_pipe();     // called in child process, so not use lock
}

/* This is for read in sync mode in main thread in redis process */
static robj* direct_read_one_key_val_from_rocksdb(const int dbid, const sds key)
{
    serverAssert(child_process_id == 0);

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

/* This is for read in sync mode in main thread in redis process */
static sds direct_read_one_field_val_from_rocksdb(const int dbid, const sds hash_key, const sds field)
{
    serverAssert(child_process_id == 0);

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

/* This is for read in sync mode in main thread in redis process */
static robj* read_from_disk_for_key_in_redis_process(const int dbid, const sds key)
{
    serverAssert(child_process_id == 0);

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

/* This is for read in sync mode in main thread in redis process 
 * For hash_key, some fields are in disk, we need create an object value for the whole key
 */
static robj* read_from_disk_for_whole_key_of_hash_in_redis_process(const int dbid, const sds hash_key)
{
    serverAssert(child_process_id == 0);

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

/* This is for read in sync mode in main thread in redis process */
static robj* read_from_disk_in_redis_process(const int have_field_in_disk, const int dbid, const sds key)
{
    serverAssert(child_process_id == 0);

    if (have_field_in_disk)
    {
        return read_from_disk_for_whole_key_of_hash_in_redis_process(dbid, key);
    }
    else
    {
        return read_from_disk_for_key_in_redis_process(dbid, key);
    }
}

/* This is run in child process */
static robj* read_from_disk_in_child_process(const int have_field_in_disk, const int dbid, const sds key)
{
    serverAssert(child_process_id != 0);

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

/* Called in service thread.
 * 
 * It needs to get the serialized obj in child_ring_buf and snapshot.
 * If failed, return NULL.
 */
static sds get_data_in_service_thread_for_child_process(const int dbid, const char* key, const size_t key_len)
{
    serverLog(LL_NOTICE, "prepare to response, dbid = %d, key_len = %zu, key = %s",
                          dbid, key_len, key);

    UNUSED(dbid);
    UNUSED(key);
    UNUSED(key_len);

    return sdsnew("psuedo_data_for_child_process");
}

/* Called in service thread. 
 *
 * When service thread finds it needs read the whole request from the buf, call to here.
 * The whole request length is request_len.
 * 
 * dealwith_request() will try to generate the real request by static sds reqeust from buf.
 * If the reqeust is generated, i.e., sdslen(rqeust) == request_len, 
 * it will call response_value() to read the val for the rock_key, i.e., the request
 * 
 * Return:
 * 1. if any error happen, return -1. (the caller will return and free request_for_service_thread)
 * 2. if need more data from the next buf, return 0.
 * 3. if successfully finish a response for a request, return 1. 
 */
static int dealwith_request(const size_t request_len, char *buf, size_t buf_len)
{
    int ret = -1;
    sds response = NULL;

    if (request_for_service_thread == NULL)
    {
        request_for_service_thread = sdsempty();
        request_for_service_thread = sdsMakeRoomFor(request_for_service_thread, request_len);
    }

    if (buf_len > request_len || sdslen(request_for_service_thread) + buf_len > request_len)
    {
        serverLog(LL_WARNING, "read too much data for a reqeust");
        goto cleanup;
    }

    if (buf_len != 0)
        // NOTE: buf_len could be zero
        request_for_service_thread = sdscatlen(request_for_service_thread, buf, buf_len);

    if (sdslen(request_for_service_thread) < request_len)
        return 0;       // need more data

    serverAssert(sdslen(request_for_service_thread) == request_len);

    // the request is dbid(one byte) + key
    if (request_len == 1)
    {
        serverLog(LL_WARNING, "key is empty");
        goto cleanup;
    }

    const int dbid = *((unsigned char*)request_for_service_thread);
    if (dbid < 0  || dbid >= server.dbnum)
    {
        serverLog(LL_WARNING, "dbid is illegal, dbid = %d", dbid);
        goto cleanup;
    }

    response = get_data_in_service_thread_for_child_process(dbid, request_for_service_thread+1, request_len-1);
    if (response == NULL)
    {
        serverLog(LL_WARNING, "can not get response for chiild process in service thread!");
        goto cleanup;
    }

    // send back the response to child process by writing to pipe write end of response
    ssize_t write_res;

    const size_t response_len = sdslen(response);
    serverAssert(response_len <= SSIZE_MAX);
    write_res = write(pipe_response[1], &response_len, sizeof(size_t));
    if (write_res < 0 || (size_t)write_res != sizeof(size_t))
    {
        serverLog(LL_WARNING, "write response head(size) failed, write_res = %zu", write_res);
        goto cleanup;
    }

    if (response_len == 0)
    {
        ret = 1;        // no need to write the content. It is sucessful.
        goto cleanup;
    }

    write_res = write(pipe_response[1], response, response_len);
    if (write_res < 0 || (size_t)write_res != response_len)
    {
        serverLog(LL_WARNING, "write response content failed, write_res = %zu", write_res);
        goto cleanup;
    }

    ret = 1;    // it is sucessful

cleanup:
    sdsfree(response);

    // for next request
    sdsfree(request_for_service_thread);
    request_for_service_thread = NULL; 

    return ret;
}   

/* The main entrance for rdb/aof service thread
 */
static void* service_thread_main(void *arg)
{
    UNUSED(arg);

    // waiting the child process to start 
    // and the snapshot are all ready from the main thread of redis process
    // by the signal from main thread (and response to the signal also in main thread)
    // so child process copy all the valid pipe
    // because before the service thread start, mutex_main_and_service will be locked
    // and after the child proocess has started, the main thread will unlock.
    // The redinesss of snapshot for RocksDB and ring buffer work the same way.

    // NOTE: in the same time, the main thread could use cancel_service_thread
    //       to notify service thread should exit
    while (1)
    {
        const int check = pthread_mutex_trylock(&mutex_main_and_service);
        if (check == 0)
        {   
            // we can get lock, it means service thread can go on to work
            // with the sync of the pipes and snapshots
            serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
            break;
        }
        else
        {
            serverAssert(check == EBUSY);

            int is_cancel = 0;
            atomicGet(cancel_service_thread, is_cancel);
            if (is_cancel)
                return NULL;     // exit the service thread

            usleep(1);      // then try lock again
        }
    }

    // then we guarantee that the sync of snapshots are ready
    serverAssert(snapshot != NULL);

    // we can close unused pipe here 
    serverAssert(pthread_mutex_lock(&mutex_main_and_service) == 0);
    close_not_used_pipe_in_service_thread();
    serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);

    serverLog(LL_NOTICE, "service thread starts to work (with snapshots) for child process!");

    #define PIPE_READ_BUF_LEN 64
    char buf[PIPE_READ_BUF_LEN];
    
    // char len_buf[sizeof(size_t)];
    char tmp[256+sizeof(size_t)];
    char *len_buf = tmp+256;
    int len_index = 0;

    size_t request_len = 0;

    while (1)
    {
        const ssize_t read_res = read(pipe_request[0], buf, PIPE_READ_BUF_LEN);
        if (read_res < 0)
        {
            serverLog(LL_WARNING, "read error from pip in service thread! ret = %zu", read_res);
            break;
        } 
        else if (read_res == 0)
        {
            serverLog(LL_WARNING, "read EOF from pipe in service thread, normal exit.");
            break;
        } 

        // We really read something
        char *data = buf;
        size_t data_len = (size_t)read_res;

        if (request_len == 0)
        {
            // first, we need read the request_len
            const int more_for_len = sizeof(size_t) - len_index;
            serverAssert(more_for_len > 0 && more_for_len <= (int)sizeof(size_t));

            if (read_res < (ssize_t)more_for_len)
            {
                // not enough for the request_len
                memcpy(len_buf+len_index, buf, read_res);
                len_index += (int)read_res;
            }
            else
            {
                // we can get request_len
                memcpy(len_buf+len_index, buf, more_for_len);
                request_len = *((size_t*)len_buf);
                
                if (request_len == 0)
                {
                    serverLog(LL_WARNING, "service thread read request_len == 0!");
                    break;
                }

                len_index = 0;  // for next request_len

                data += more_for_len;
                serverAssert(data_len >= (size_t)more_for_len);
                data_len -= more_for_len;
            }
        }

        if (request_len != 0)
        {
            // if already read request_len, we deal with the real request
            const int deal_res = dealwith_request(request_len, data, data_len);

            if (deal_res == -1)
            {
                // error when deal request
                serverLog(LL_WARNING, "dealwith_request() return -1, it is an error!");
                break;
            }
            else if (deal_res == 1)
            {
                // finish a response for a request
                serverLog(LL_NOTICE, "we finish a request.");
                request_len = 0;        // for next request_len
                serverAssert(len_index == 0);
            }
            // else go on read for more data for an request data
        }
    }

    // reclaim all resource before service threa exit
    // NOTE: need lock to guarantee the sync
    sdsfree(request_for_service_thread);
    request_for_service_thread = NULL;

    serverAssert(pthread_mutex_lock(&mutex_main_and_service) == 0);
    rocksdb_release_snapshot(rockdb, snapshot);
    snapshot = NULL;
    clear_all_resource_of_pipe();
    clear_snapshot_of_ring_buffer();
    serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);

    return NULL;
}
