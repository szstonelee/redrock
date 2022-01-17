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
#include "rock_hash.h"
#include "rock_evict.h"

#include <unistd.h>
#include <pthread.h>

/* If in main redis proocess, it is zero. otherwise, it is the child process id
 * For redis process, it is like read-only.
 * For child process, it is only for main thread.
 * So no need for mutex safe guard.
 */
static int child_process_id = 0;

/* Pipe for child process and service thread in redis process for rdb or aof
 * They are accessed by main thread and service thread of redis process 
 * and main thread of child process.
 * So the logic and mutex_main_and_service need to guarantee the visibility of those threads.
 * 
 * For redis process, 
 * 1. pipe creatation is inited in main thread
 *    before service thread start and child process forked.
 * 2. The close of unused pipes is after the mutex_main_and_service 
 *    in service thread because child process needs copy (and share) all the unclosed pipes.
 * So mutex_main_and_service is used for the purpose in redis process.
 * 
 * For child process, the close of unused pipes is at the time when it starts.
 * When service thread exit and child process exit, they need close all these file descriptors.
 */
static int pipe_request[2];
static int pipe_response[2];

/* The service_thread is only accessed by main thread in redis process
 * to control the service thread like 
 *
 * 1. Terminate the service thread in terminate_service_thread() if something wrong.
 * 
 * 2. Check whether the service thread is running by pthread_tryjoin_np() 
 *    in on_start_rdb_aof_process().
 *    The first time does not need to check becaue service_thread is NULL,
 *    after the first time, the service_thread is always 
 *    the current runnning service thread or the previous exited service thread. 
 * 
 * So no need to use mutex for safeguard. 
 */
static pthread_t *service_thread = NULL;         

/* The mutex_main_and_service has these purpose:
 *
 * 1. Safeguard(sync) pipe between main thread and service thread in redis process

 * 2. Safeguard(sync) snapshots, i.e., snapshot of ring buffer and RocksDB,
 *    between main thread and service thread in redis process
 * 
 * 3. Make pipe changes sync between redis process and child process.
 *    After the service thread start, it will try to get the lock which has been locked 
 *    by main thread who inits something before child process start.
 *    After everything is ready, especially when main thread guarantees 
 *    that child process has been started, main thread unlock the mutex_main_and_service
 *    like giving a signal for service thread.
 *    So the service thread get the lcok and know it can go on to work, 
 *    like closing unused pipes (in redis process).
 */
static pthread_mutex_t mutex_main_and_service;     

/* The following three variables are snapshots for ring buffer and RockksDB,
 * They are only needed and modified (except first time initialization) by service thread.
 * They are guarded by mutex_main_and_service because the first initialization is in main thread.
 * (when duplicating from ring buffer, also guarded by spin lock in rock_write.c)
 */
static const rocksdb_snapshot_t *snapshot = NULL;  
static sds child_ringbuf_keys[RING_BUFFER_LEN];
static sds child_ringbuf_vals[RING_BUFFER_LEN];

/* It is only used in servie thread so no need to use safeguard.
 *
 * It is for the current request sent from child process.
 * 
 * After the response (or error) of the request from service thread,
 * it is freed and set to NULL again to wait for next request.
 */
static sds request_content_for_service_thread = NULL;

/* The main thread in redis process use the cancel_service_thread to nofify
 * the service thread to exit when the mutex_main_and_service is locked.
 * It is a terminination flag for service thread.
 */
static redisAtomic int cancel_service_thread;

/* Called when redis start in main thread.
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

    for (int i = 0; i < RING_BUFFER_LEN; ++i)
    {
        child_ringbuf_keys[i] = NULL;
        child_ringbuf_vals[i] = NULL;
    }       

    serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
}

/* After service thread started succesfully,
 * it needs to close the unecesssary file descriptor of pipe, including:
 * 1. request write end
 * 2. response read end
 * 
 * NOTE1: It must guarantee that child process has already started
 *        because here will modify the pipe file descriptor (set to -1)
 * 
 * NOTE2: The caller must guarantee in lock mode.
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
 * close the unecessary file descriptor of pipe including:
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

/* Only called in child process when child process just starts.
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

/* Called in child process to send a request of a whole key by dbid and key.
 * 
 * The encoding of the request is:
 * 1. header: size of the following contnent
 * 2. content: follwing the header
 * 
 * For 2, the content:
 * is encoded of 
 * 2-1) one byte of true indicating it is for whole key
 * 2-2) one byte of dbid 
 * 2-3) the key.
 * 
 * Return True(1) if sucess, otherwise false(0).
 *
 * NOTE: For receiver (in service thread), 
 *       it only needs to save the content in the request_content_for_service_thread.
 */
static int send_request_for_whole_key_in_child_process(const int dbid, const sds key)
{
    serverAssert(child_process_id != 0);
    serverAssert(dbid >= 0 && dbid < server.dbnum && key);

    sds content = sdsempty();
    content = sdsMakeRoomFor(content, 1 + 1 + sdslen(key));

    // 2-1
    const char is_whole_key = 1;
    content = sdscatlen(content, &is_whole_key, 1);

    // 2-2
    const unsigned char c_dbid = (unsigned char)dbid;
    content = sdscatlen(content, &c_dbid, 1);

    // 2-3
    content = sdscatlen(content, key, sdslen(key));

    ssize_t write_res;
    size_t content_sz = sdslen(content);
    int ret = 0;

    // first send header
    write_res = write(pipe_request[1], &content_sz, sizeof(size_t));
    if (write_res != sizeof(size_t))
        goto reclaim;

    // then the content
    write_res = write(pipe_request[1], content, sdslen(content));
    if (write_res < 0 || (size_t)write_res != sdslen(content))
        goto reclaim;

    ret = 1;    // succesfully

reclaim:
    if (ret != 1)
        serverLog(LL_WARNING, "send_request_for_whole_key_in_child_process() write failed!");

    sdsfree(content);
    return ret;
}

/* Called in child process to send a request of a field's value by dbid and key and field.
 *
 * The encoding of the request is:
 * 1. header: size of the following contnent
 * 2. content: follwing the header
 * 
 * For 2, the content:
 * 2-1) is encoded of one byte of false indicating it is for one field only
 * 2-2) one byte of dbid 
 * 2-3) key len
 * 2-4) the key
 * 2-5) field len
 * 2-6) the field.
 * 
 * Return True(1) if sucess, otherwise false(0).
 */
static int send_request_for_field_in_child_process(const int dbid, const sds key, const sds field)
{
    serverAssert(child_process_id != 0);
    serverAssert(dbid >= 0 && dbid < server.dbnum && key);

    sds content = sdsempty();
    content = sdsMakeRoomFor(content, 1 + 1 + sizeof(size_t) + sdslen(key) + sizeof(size_t) + sdslen(field));

    // 2-1
    const char is_whole_key = 0;
    content = sdscatlen(content, &is_whole_key, 1);

    // 2-2
    const unsigned char c_dbid = (unsigned char)dbid;
    content = sdscatlen(content, &c_dbid, 1);

    // 2-3
    const size_t key_len = sdslen(key);
    content = sdscatlen(content, &key_len, sizeof(size_t));

    // 2-4
    content = sdscatlen(content, key, key_len);

    // 2-5
    const size_t field_len = sdslen(field);
    content = sdscatlen(content, &field_len, sizeof(size_t));

    // 2-6
    content = sdscatlen(content, field, field_len);

    ssize_t write_res;
    size_t content_sz = sdslen(content);
    int ret = 0;

    // first send header
    write_res = write(pipe_request[1], &content_sz, sizeof(size_t));
    if (write_res != sizeof(size_t))
        goto reclaim;

    // then the content
    write_res = write(pipe_request[1], content, sdslen(content));
    if (write_res < 0 || (size_t)write_res != sdslen(content))
        goto reclaim;

    ret = 1;    // succesfully

reclaim:
    if (ret != 1)
        serverLog(LL_WARNING, "send_request_for_field_in_child_process() write failed!");

    sdsfree(content);
    return ret;
}

/* Called in child process.
 *
 * After the chiild process send a request, it will wait the response.
 * return NULL if error, otherwise, the content of sds
 * 
 * It could be a response of whole key or field.
 * 
 * For whole key, the response is the serialized string for the robj.
 * 
 * For field, the response is the value for the field.
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
        
        if (read_res == 0)
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
            // read repeatly until the full response generated
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

#ifdef __APPLE__
/* Apple MacOS does not support pthread_timedjoin_np(),
 * so we provide a compatable way to do it.
 * check https://stackoverflow.com/questions/11551188/alternative-to-pthread-timedjoin-np
 */

struct args {
    int joined;
    pthread_t td;
    pthread_mutex_t mtx;
    pthread_cond_t cond;
    void **res;
};

static void *waiter(void *ap)
{
    struct args *args = ap;
    pthread_join(args->td, args->res);
    pthread_mutex_lock(&args->mtx);
    args->joined = 1;
    pthread_mutex_unlock(&args->mtx);
    pthread_cond_signal(&args->cond);
    return 0;
}

int pthread_timedjoin_np(pthread_t td, void **res, struct timespec *ts)
{
    pthread_t tmp;
    int ret;
    struct args args = { .td = td, .res = res };

    pthread_mutex_init(&args.mtx, 0);
    pthread_cond_init(&args.cond, 0);
    pthread_mutex_lock(&args.mtx);

    serverAssert(pthread_create(&tmp, 0, waiter, &args) == 0);

    do ret = pthread_cond_timedwait(&args.cond, &args.mtx, ts);
    while (!args.joined && ret != ETIMEDOUT);

    pthread_mutex_unlock(&args.mtx);

    pthread_cancel(tmp);
    pthread_join(tmp, 0);

    pthread_cond_destroy(&args.cond);
    pthread_mutex_destroy(&args.mtx);

    return args.joined ? 0 : ret;
}
#endif

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
        #ifdef __APPLE__
        struct timespec to;
        clock_gettime(CLOCK_MONOTONIC, &to);
        to.tv_sec += 1;
        const int check = pthread_timedjoin_np(*service_thread, NULL, &to);
        #else
        const int check = pthread_tryjoin_np(*service_thread, NULL);  // non-blocking call
        #endif
        if (check != 0)
        {
            #ifndef __APPLE__
            serverAssert(check == EBUSY);
            #endif
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

/* This is called in service thread to find the value in snapshot 
 * of ring buffer for rock_key.
 * If found, return the value.
 * Otherwise, return NULL.
 */
static sds read_from_snapshot_of_ring_buffer(const sds rock_key)
{
    const size_t rock_len = sdslen(rock_key);
    sds found = NULL;
    for (int i = 0; i < RING_BUFFER_LEN; ++i)
    {
        if (child_ringbuf_keys[i] == NULL)
            break;

        if (sdslen(child_ringbuf_keys[i]) == rock_len && sdscmp(child_ringbuf_keys[i], rock_key) == 0)
        {
            found = child_ringbuf_vals[i];
            break;
        }
    }
    return found;
}

/* This is called in service thread to find the value in snapshot 
 * of RocksDB for rock_key.
 * If found, return the value.
 * Otherwise, return NULL.
 */
static sds read_from_snapshot_of_rocksdb(const sds rock_key)
{
    size_t db_val_len;
    char *err = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    serverAssert(snapshot != NULL);
    rocksdb_readoptions_set_snapshot(readoptions, snapshot);
    char *db_val = rocksdb_get(rockdb, readoptions, rock_key, sdslen(rock_key), &db_val_len, &err);
    rocksdb_readoptions_destroy(readoptions);

    if (err)
        serverPanic("read_from_snapshot_of_rocksdb(), err = %s", err);

    if (db_val == NULL)
        // NOT FOUND, but it is illegal, but we make the caller deal with that
        return NULL;
    
    return sdsnewlen(db_val, db_val_len);
}

static sds read_from_snapshot_first_ringbuf_then_rocksdb(const sds rock_key)
{
    const sds val = read_from_snapshot_of_ring_buffer(rock_key);
    if (val != NULL)
        return val;
    
    return read_from_snapshot_of_rocksdb(rock_key);
}

/* This is for client/server mode in service thead in redis process for one whole key's value */
static sds read_from_snapshot_for_whole_key_in_service_thread(const int dbid, const char *key, const size_t key_len)
{
    sds rock_key = sdsnewlen(key, key_len);
    rock_key = encode_rock_key_for_db(dbid, rock_key);

    const sds val = read_from_snapshot_first_ringbuf_then_rocksdb(rock_key);

    sdsfree(rock_key);

    return val;
}

/* This is for client/server mode in service thead in redis process for one field's value */
static sds read_from_snapshot_for_hash_field_in_service_thread(const int dbid, const sds key, const sds field)
{
    sds rock_key = sdsdup(key);
    rock_key = encode_rock_key_for_hash(dbid, rock_key, field);

    const sds val = read_from_snapshot_first_ringbuf_then_rocksdb(rock_key);

    sdsfree(rock_key);

    return val;
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

/* Called in service thread when a request is totally received
 * and we need get the real data from snapshots and return it.
 * 
 * We need parse the reqeust to know 
 * 1. it is for the whole key
 * 2. or it is for one field
 * 
 * and then, we will get the data from snapshot
 * For whole key, it is the serialized robj as sds
 * For field, it is the value of the field as sds.
 * 
 * If failed, return NULL.
 */
static sds get_data_in_service_thread_for_child_process(const sds request)
{
    if (sdslen(request) < 1 + 1)
    {
        // at least two bytes, one is flag of whole key or field, one byte is for dbid
        serverLog(LL_WARNING, "get_data_in_service_thread_for_child_process() len too small");
        return NULL;
    }

    const int is_for_whole_key = request[0];
    serverAssert(is_for_whole_key == 1 || is_for_whole_key == 0);
    const int dbid = ((unsigned char*)request)[1];
    serverAssert(dbid >= 0 && dbid < server.dbnum);

    char *p = request + 2;
    size_t p_len = sdslen(request) - 2;

    if (is_for_whole_key)
    {
        // whole key does not need to parse
        return read_from_snapshot_for_whole_key_in_service_thread(dbid, p, p_len);
    }
    else
    {
        // parse for key and field
        sds key = NULL;
        sds field = NULL;

        if (p_len < sizeof(size_t))
            goto err_of_field;

        const size_t key_len = *((size_t*)p);
        p += sizeof(size_t);
        p_len -= sizeof(size_t);

        if (p_len < key_len)
            goto err_of_field;
        
        key = sdsnewlen(p, key_len);
        p += key_len;
        p_len -= key_len;

        if (p_len < sizeof(size_t))
            goto err_of_field;

        const size_t field_len = *((size_t*)p);
        p += sizeof(size_t);
        p_len -= sizeof(size_t);

        if (p_len != field_len)
            goto err_of_field;

        field = sdsnewlen(p, field_len);
        
        const sds data = read_from_snapshot_for_hash_field_in_service_thread(dbid, key, field);
        sdsfree(key);
        sdsfree(field);
        return data;

err_of_field:
        serverLog(LL_WARNING, "get_data_in_service_thread_for_child_process() parse key and field failed!");
        sdsfree(key);
        sdsfree(field);
        return NULL;        
    }
}

/* This is called in child process.
 * When child process need the value of a key and found 
 * 1. the whole key's value is in disk
 * 2. or some field's value are in disk
 * 
 * It weill call here to get the value (return robj*)
 * 
 * We must use client/service mode by using a pipe to get the value.
 * 
 * If failed, return NULL which is impossible but the child process (caller) needs to exit.
 */
static robj* read_val_of_disk_in_child_process(const int have_field_in_disk, const int dbid, const sds key)
{
    serverAssert(child_process_id != 0);

    if (!have_field_in_disk)
    {
        // for whole key
        if (!send_request_for_whole_key_in_child_process(dbid, key))
            return NULL;

        sds response = receive_response_in_child_process();
        if (response == NULL)
            return NULL;

        robj *o = unmarshal_object(response);
        sdsfree(response);
        return o;
    }
    else
    {
        // We need find create a new hash from the source of the memory hash (in child process)
        // For field's value in memory (in child proces) just copy them.
        // For field's value in disk, use client/server mode to send to service thread
        // one by one
        redisDb *db = server.db + dbid;
        dictEntry *de_db = dictFind(db->dict, key);
        serverAssert(de_db);
        robj *o_de = dictGetVal(de_db);
        serverAssert(!is_rock_value(o_de));
        serverAssert(o_de->type == OBJ_HASH && o_de->encoding == OBJ_ENCODING_HT);
        dict *hash = o_de->ptr;

        dict *new_hash = dictCreate(&hashDictType, NULL);
        if (dictSize(hash) > DICT_HT_INITIAL_SIZE)
            dictExpand(new_hash, dictSize(hash));
        int all_fields_not_in_disk = 1;
        dictIterator *di_hash = dictGetIterator(hash);
        dictEntry *de_hash;
        while ((de_hash = dictNext(di_hash)))
        {
            const sds val = dictGetVal(de_hash);
            const sds field = dictGetKey(de_hash);
            if (val != shared.hash_rock_val_for_field)
            {
                // real val in child process memory, just copy it to new_hash
                const sds copy_field = sdsdup(field);
                const sds copy_val = sdsdup(val);
                dictAdd(new_hash, copy_field, copy_val);
            }
            else
            {
                // we need client/server mode to get the field's value 
                // if the field's value is in disk by checking child process memory
                all_fields_not_in_disk = 0;
                if (!send_request_for_field_in_child_process(dbid, key, field))
                {
                    dictRelease(new_hash);
                    dictReleaseIterator(di_hash);
                    return NULL;
                }
                const sds response = receive_response_in_child_process();
                if (response == NULL)
                {
                    dictRelease(new_hash);
                    dictReleaseIterator(di_hash);
                    return NULL;
                }
                const sds copy_field = sdsdup(field);
                dictAdd(new_hash, copy_field, response);
            }
        }
        dictReleaseIterator(di_hash);

        serverAssert(!all_fields_not_in_disk);

        robj *new_o = createObject(OBJ_HASH, new_hash);
        new_o->encoding = OBJ_ENCODING_HT;
        return new_o;
    }
}

/* Check whether the value needs to get from RocksDB if the o is in RocksDB.
 * If o is not roock value or not in rock_hash, 
 *    we return the input o, 
 * otherwise, we return the value from disk, i.e, the o_disk.
 *
 * It can be in main process of Redis (in main thread which is sync way) 
 * or child process for rdb or aof.
 * 
 * If in main thread of redis process, we read whole data from ring buffer and RocksdB.
 * 
 * If in main thread of child process, we need client/server mode.
 */
robj* get_value_if_exist_in_rock_for_rdb_afo(const robj *o, const int dbid, const sds key)
{
    redisDb *db = server.db + dbid;

    // We need check current memory (for redis process or child process)
    // to know whether it is for disk
    // It could be a whole key of rock value or some fields in rocksdb
    int have_field_in_disk = 0;
    if (!is_rock_value(o))
    {
        dictEntry *de = dictFind(db->rock_hash, key);
        if (de == NULL)
        {
            // not rock value and not exist in rock hash, it is a concrete value
            return (robj*)o;   
        }
        else
        {
            serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);
            dict *lrus = dictGetVal(de);
            dict *hash = o->ptr;

            if (dictSize(hash) == dictSize(lrus))
            {
                // all fields not in disk for a hash (even the key is in rock_hash), 
                // it is a concrete value
                return (robj*)o;       
            }
            else
            {
                have_field_in_disk = 1;
            }
        }
    }

    // We need create a temporary o_disk and return it
    // The caller has the responsibility to releasse o_disk
    robj *o_disk = NULL;
    if (child_process_id == 0)
    {
        // in redis process, use sync way
        o_disk = read_from_disk_in_redis_process(have_field_in_disk, dbid, key);
    }
    else
    {
        // in child process, use client/server way
        o_disk = read_val_of_disk_in_child_process(have_field_in_disk, dbid, key);
        if (o_disk == NULL)
        {
            serverLog(LL_WARNING, "child process get value by pipe failed, something wrong, must exit!");
            exit(1);
        }
    }

    serverAssert(o_disk != NULL);
    return o_disk;
}

/* Called in service thread. 
 *
 * When service thread finds it needs read the content of a request from the buf, call to here.
 * The whole contentn length is the content_len.
 * 
 * dealwith_request() will try to generate the real coontent of a request 
 * in request_content_for_service_thread.
 * 
 * If the the real coontent  is generated, i.e., sdslen(request_content_for_service_thread) == content_len, 
 * it will call response_value() to read the val for the rock_key, i.e., the request
 * 
 * Return:
 * 1. if any error happen, return -1. (the caller will exit and free request_content_for_service_thread)
 * 2. if need more data from the next buf, return 0.
 * 3. if successfully finish a response for a request, return 1. 
 */
static int dealwith_request(const size_t content_len, char *buf, size_t buf_len)
{
    int ret = -1;
    sds response = NULL;

    if (request_content_for_service_thread == NULL)
    {
        request_content_for_service_thread = sdsempty();
        request_content_for_service_thread = sdsMakeRoomFor(request_content_for_service_thread, content_len);
    }

    if (buf_len > content_len || sdslen(request_content_for_service_thread) + buf_len > content_len)
        goto cleanup;

    if (buf_len != 0)
        // NOTE: buf_len could be zero
        request_content_for_service_thread = sdscatlen(request_content_for_service_thread, buf, buf_len);

    if (sdslen(request_content_for_service_thread) < content_len)
    {
        ret = 0;       // need more data
        goto cleanup;
    }

    // We got the whole content
    serverAssert(sdslen(request_content_for_service_thread) == content_len);

    // We need parse the request_content_for_service_thread 
    // because it could be for the whole key or one field
    response = get_data_in_service_thread_for_child_process(request_content_for_service_thread);
    if (response == NULL)
        goto cleanup;

    // send back the response to child process by writing to pipe write end of response
    ssize_t write_res;
    const size_t response_len = sdslen(response);
    serverAssert(response_len > 0 && response_len <= SSIZE_MAX);

    // write header first of the size of the response
    write_res = write(pipe_response[1], &response_len, sizeof(size_t));
    if (write_res < 0 || (size_t)write_res != sizeof(size_t))
        goto cleanup;

    // then write the response itself
    write_res = write(pipe_response[1], response, response_len);
    if (write_res < 0 || (size_t)write_res != response_len)
        goto cleanup;

    ret = 1;    // it is sucessful

cleanup:
    if (ret == -1)
        serverLog(LL_WARNING, "dealwith_request() found an error!");

    if (ret != 0)
    {
        // for next reqeust if not need more data
        sdsfree(request_content_for_service_thread);
        request_content_for_service_thread = NULL;
    }

    sdsfree(response);

    return ret;
}   


/* Called in service thread.
 * It will wait the main thread unlok mutex_main_and_service
 * so the service thread can go on to work. It returns 1.
 * 
 * If in the waiting period, it finds main thread wants servict thread
 * to abort by the atomic cancel_service_thread, 
 * it return 0 meaning it can not go on.
 */
static int service_thread_wait_until_can_work()
{
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
                return 0;     // need to exit the service thread

            usleep(1);      // then try lock again
        }
    }

    return 1;   // can go on to work
}

/* Called in service thread to init including
 * 1. close unused pipe
 * 2. create snapshots of ring buffer and RocksDB
 */
static void init_resource_in_service_thread()
{
    serverAssert(pthread_mutex_lock(&mutex_main_and_service) == 0);
    // we can close unused pipe here 
    close_not_used_pipe_in_service_thread();
    // creatation of snapshot
    // NOTE: we need call ring buffer first, because write thread will 
    //       change ring buffer, we must guarantee no data loss for snapshot
    serverAssert(snapshot == NULL);
    serverAssert(child_ringbuf_keys[0] == NULL);
    create_snapshot_of_ring_buf_for_child_process(child_ringbuf_keys, child_ringbuf_vals);
    snapshot = rocksdb_create_snapshot(rockdb);
    serverAssert(snapshot != NULL);
    serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
}

/* Called in service thread before normal exit
 * to recaim the above resource */
static void reclaim_resource_in_service_thread()
{
    sdsfree(request_content_for_service_thread);
    request_content_for_service_thread = NULL;
   
    serverAssert(pthread_mutex_lock(&mutex_main_and_service) == 0);
    rocksdb_release_snapshot(rockdb, snapshot);
    snapshot = NULL;
    clear_all_resource_of_pipe();
    clear_snapshot_of_ring_buffer();
    serverAssert(pthread_mutex_unlock(&mutex_main_and_service) == 0);
}

/* The main entrance for rdb/aof service thread
 * as the server for child process by pipe communication.
 */
static void* service_thread_main(void *arg)
{
    UNUSED(arg);

    if (service_thread_wait_until_can_work() == 0)
        return NULL;    // exit service thread by abortion
    
    init_resource_in_service_thread();

    serverLog(LL_NOTICE, "service thread starts to work (with snapshots) for child process!");

    #define PIPE_READ_BUF_LEN 64
    char buf[PIPE_READ_BUF_LEN];
    
    char header_buf[sizeof(size_t)];
    int header_index = 0;

    size_t content_len = 0;

    while (1)
    {
        const ssize_t read_res = read(pipe_request[0], buf, PIPE_READ_BUF_LEN);
        if (read_res < 0)
        {
            serverLog(LL_WARNING, "read error from pip in service thread! ret = %zu", read_res);
            break;
        } 

        if (read_res == 0)
        {
            serverLog(LL_WARNING, "read EOF from pipe in service thread, normal exit.");
            break;
        } 

        // We really read something
        char *data = buf;
        size_t data_len = (size_t)read_res;

        if (content_len == 0)
        {
            // if header is not ready, parse head first
            const int more_for_header = sizeof(size_t) - header_index;
            serverAssert(more_for_header > 0 && more_for_header <= (int)sizeof(size_t));

            if (read_res < (ssize_t)more_for_header)
            {
                // not enough for the header
                memcpy(header_buf+header_index, buf, read_res);
                header_index += (int)read_res;
            }
            else
            {
                // we can get header and the content_len
                memcpy(header_buf+header_index, buf, more_for_header);
                content_len = *((size_t*)header_buf);
                header_index = 0;  // for next header
                
                if (content_len == 0)
                {
                    serverLog(LL_WARNING, "service thread read content_len == 0!");
                    break;
                }                

                data += more_for_header;
                serverAssert(data_len >= (size_t)more_for_header);
                data_len -= more_for_header;
            }
        }

        if (content_len != 0)
        {
            // if already read content_len, we deal with the real content
            const int deal_res = dealwith_request(content_len, data, data_len);

            if (deal_res == -1)
            {
                // error when deal request
                serverLog(LL_WARNING, "dealwith_request() return -1, it is an error!");
                break;
            }
            else if (deal_res == 1)
            {
                // finish a response for a request
                // serverLog(LL_NOTICE, "we finish a request.");
                content_len = 0;        // for next content
                serverAssert(header_index == 0);
            }
            // else go on read for more data for an request data
        }
    }

    reclaim_resource_in_service_thread();

    return NULL;    // exit service thread normally
}

/* When redis server starts, it loads rdb file in two ways.
 * 1. is directly from rdb file
 * 2. is from aof file which has part of rdb content
 * 
 * After the loading, we need call here to init the rock_hash and rock_evict
 * 
 * For the left of aof file which is one command executed one by one, 
 * it does not need call here but we need prepare for it (by calling here)
 */
void on_after_load_rdb_backup()
{
    init_rock_hash_before_enter_event_loop();  
    // NOTE: must follow init_rock_hash_before_enter_event_loop()
    init_rock_evict_before_enter_event_loop();  
}
