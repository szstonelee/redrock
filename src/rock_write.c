#include "rock_write.h"
#include "rock.h"
#include "rock_marshal.h"
#include "rock_read.h"
#include "rock_hash.h"

// #include <stddef.h>
// #include <assert.h>


/* Write Spin Lock for Apple OS and Linux */
#if defined(__APPLE__)

    #include <os/lock.h>
    static os_unfair_lock w_lock;

    static void init_write_spin_lock() 
    {
        w_lock = OS_UNFAIR_LOCK_INIT;
    }

    inline static void rock_w_lock() 
    {
        os_unfair_lock_lock(&w_lock);
    }

    inline static void rock_w_unlock() 
    {
        os_unfair_lock_unlock(&w_lock);
    }

#else   // Linux

    #include <pthread.h>
    static pthread_spinlock_t w_lock;

    static void init_write_spin_lock() 
    {
        pthread_spin_init(&w_lock, 0);
    }    

    inline static void rock_w_lock() 
    {
        int ret = pthread_spin_lock(&w_lock);
        serverAssert(ret == 0);
    }
   
    inline static void rock_w_unlock() 
    {
        int ret = pthread_spin_unlock(&w_lock);
        serverAssert(ret == 0);
    }

#endif

pthread_t rock_write_thread_id;

/* 
 * Ring Buffer as a write queue to RocksDB
 */

static sds rbuf_keys[RING_BUFFER_LEN];
static sds rbuf_vals[RING_BUFFER_LEN];
static int rbuf_s_index;     // start index in queue (include if rbuf_len != 0)
static int rbuf_e_index;     // end index in queue (exclude if rbuf_len != 0)
static int rbuf_len;         // used(available) length

/* Called by Main thread to init the ring buffer */
static void init_write_ring_buffer() 
{
    rock_w_lock();
    for (int i = 0; i < RING_BUFFER_LEN; ++i) 
    {
        rbuf_keys[i] = NULL;
        rbuf_vals[i] = NULL;
    }
    rbuf_s_index = rbuf_e_index = 0;
    rbuf_len = 0;
    rock_w_unlock();
}

/* Called by Main thread in lock mode from caller.
 * keys and vals will be ownered by ring buffer.
 * We free memory only when overwrite old and abandoned key/value
 */
static void batch_append_to_ringbuf(const int len, sds* keys, sds* vals) 
{
    // serverAssert(len > 0 && len <= RING_BUFFER_LEN - rbuf_len); // the caller guarantee

    for (int i = 0; i < len; ++i) 
    {
        const sds key = keys[i];
        const sds val = vals[i];
        serverAssert(key && val);

        if (rbuf_keys[rbuf_e_index]) 
        {
            sdsfree(rbuf_keys[rbuf_e_index]);
            serverAssert(rbuf_vals[rbuf_e_index]);
            sdsfree(rbuf_vals[rbuf_e_index]);
        }

        rbuf_keys[rbuf_e_index] = key;
        rbuf_vals[rbuf_e_index] = val;

        ++rbuf_e_index;
        if (rbuf_e_index == RING_BUFFER_LEN) 
            rbuf_e_index = 0;
    }
    rbuf_len += len;
}

/* Called by Main thread in cron to determine how much space (key number) 
 * left in ring buffer for evicting to RocksDB.
 *
 * NOTE: We need to use lock to guarantee the data race 
 *       (Write thread maybe decrease rbuf_len)
 */
int space_in_write_ring_buffer()
{
    rock_w_lock();
    const int space = RING_BUFFER_LEN - rbuf_len;
    rock_w_unlock();

    serverAssert(space >= 0 && space <= RING_BUFFER_LEN);
    return space;
}

/* Called in Main thread in cron (not directly).
 * The caller guarantees not in lock mode.
 * 
 * NOTE1: The caller must guarantee that these keys in redis db 
 *        have been set the corresponding value to rock values.
 * 
 * NOTE2: Question??? I move out the marshal code out of lock mode.
 *        The note2 is optimized to not using!!!!
 *        We serialize vals in lock mode to avoid data race.
 *        Maybe we could serialize out of lock mode.
 *        But the guarantee of data integrity is very important 
 *        and main thread can use more time (all operations are for memory),
 *        so all are done in lock mode.
 * 
 * NOTE3: We will release the objs.
 *        The keys will be encoded with dbid and be tansfered the ownership 
 *        to ring buffer by calling batch_append_to_ringbuf().
 *        So the caller needs to do :
 *           1. duplicate the keys from the source of keys in Redis db
 *           2. not use keys anymore
 *           3. not use objs anymore
 */
static void write_batch_append_and_abandon(const int len, const int *dbids, sds *keys, robj **objs)
{
    serverAssert(len > 0);

    sds vals[RING_BUFFER_LEN];

    for (int i = 0; i < len; ++i)
    {
        sds key = keys[i];
        key = encode_rock_key_for_db(dbids[i], key);
        keys[i] = key;
        sds val = marshal_object(objs[i]);
        vals[i] = val;
    }

    rock_w_lock();
    serverAssert(rbuf_len + len <= RING_BUFFER_LEN);
#ifdef RED_ROCK_DEBUG
    serverAssert(debug_check_no_candidates(len, keys));
#endif
    batch_append_to_ringbuf(len, keys, vals);
    rock_w_unlock();

    // release objs
    for (int i = 0; i < len; ++i)
    {
        decrRefCount(objs[i]);
    }
}

/* Called in main thread cron and command ROCKEVICT.
 *
 * When cron() select some keys (before setting rock value), it will call here
 * to determine which keys can be evicted to RocksDB 
 * because we need exclude those keys in read_rock_key_candidates. 
 * Check rock_read.c for read_rock_key_candidates for more reference. 
 * 
 * try_evict_to_rocksdb_for_db() will set value to rock value for these matched keys.
 * Return the actual number of keys for eviction.
 * 
 * NOTE1: The caller needs to use space_in_write_ring_buffer() first
 *        to know the available space for ring buffer.
 *        The caller needs to guarantee check_len <= space.
 * 
 * NOTE2: The caller guarantee the vlaue can be evicted. (e.g. NOT stream value)
 * 
 * NOTE3: The keys must be not in rock hash.
 */
int try_evict_to_rocksdb_for_db(const int try_len, const int *try_dbids, const sds *try_keys)
{
    serverAssert(try_len > 0);

    int evict_len = 0;

    int evict_dbids[RING_BUFFER_LEN];
    sds evict_keys[RING_BUFFER_LEN];
    for (int i = 0; i < try_len; ++i)
    {
        const int dbid = try_dbids[i];
        const sds try_key = try_keys[i];

        serverAssert(!is_in_rock_hash(dbid, try_key));

        if (already_in_candidates_for_db(dbid, try_key))     // the rock_read.c API
            continue;

        // NOTE: we must duplicate for write_batch_append_and_abandon()
        evict_keys[evict_len] = sdsdup(try_key);  
        evict_dbids[evict_len] = dbid;
        ++evict_len;
    }

    robj* evict_vals[RING_BUFFER_LEN];
    for (int i = 0; i < evict_len; ++i)
    {
        redisDb *db = server.db + evict_dbids[i];
        dictEntry *de = dictFind(db->dict, evict_keys[i]);
        serverAssert(de);

        // change the value to rock value in Redis DB
        robj *v = dictGetVal(de);
        serverAssert(is_evict_value(v));        
        dictGetVal(de) = get_match_rock_value(v);
        evict_vals[i] = v;    
    }

    if (evict_len)
        write_batch_append_and_abandon(evict_len, evict_dbids, evict_keys, evict_vals);

    return evict_len;    
}

/* Called in main thread cron and command ROCKEVICTHASH
 *
 * When cron() select some fields from some hashes (before setting rock value), it will call here
 * to determine which fields can be evicted to RocksDB 
 * because we need exclude those keys in read_rock_key_candidates. 
 * Check rock_read.c for read_rock_key_candidates for more reference. 
 *
 * try_evict_to_rocksdb_for_hash() will set value to rock value for these matched fields.
 * Return the actual number of fields for eviction.
 *
  * NOTE1: The caller needs to use space_in_write_ring_buffer() first
 *        to know the available space for ring buffer.
 *        The caller needs to guarantee check_len <= space.
 *
 * NOTE2: The caller guarantee the vlaue can be evicted, i.e., the value for the field is not rock value.
 */
int try_evict_to_rocksdb_for_hash(const int try_len, const int *try_dbids, const sds *try_keys, const sds *try_fields)
{
    UNUSED(try_dbids);
    UNUSED(try_keys);
    UNUSED(try_fields);

    serverAssert(try_len > 0);

/*
    int evict_len = 0;

    int evict_dbids[RING_BUFFER_LEN];
    sds evict_keys[RING_BUFFER_LEN];
    sds evict_fields[RING_BUFFER_LEN];
*/

    return 0;
}

/* Called in main thread for command ROCKEVICT
 * If succesful, return 1.
 * Otherwise, return 0. The caller can try again because the key may be in candidates 
 *                      and it takes times for read thread to process (no recover but abandon).
 * 
 * The caller needs to guarantee the value in Redis exist and can be evicted to RocksDB.
 */
int try_evict_one_key_to_rocksdb(const int dbid, const sds key)
{
    const int space = space_in_write_ring_buffer();
    if (space == 0)
        return 0;
    
    if (try_evict_to_rocksdb_for_db(1, &dbid, &key) == 0)
    {
        return 0;
    }
    else
    {
        return 1;
    }
}

/* Called by write thread.
 * If nothing written to RocksDB, return 0. Otherwise, the number of key written to db.
 */
static int write_to_rocksdb()
{
    // Make lock as short as possible in write thread
    rock_w_lock();
    if (rbuf_len == 0)
    {
        rock_w_unlock();
        return 0;
    }
    int written = rbuf_len;
    int index = rbuf_s_index;
    rock_w_unlock();
       
    rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(writeoptions, 1);      // disable WAL

    for (int i = 0; i < written; ++i) 
    {
        // Guarantee to get the updated data from Main thread up to written
        const sds key = rbuf_keys[index];
        const sds val = rbuf_vals[index];

        ++index;
        if (index == RING_BUFFER_LEN)
            index = 0;

        rocksdb_writebatch_put(batch, key, sdslen(key), val, sdslen(val));
    }

    char *err = NULL;
    rocksdb_write(rockdb, writeoptions, batch, &err);    
    if (err) 
        serverPanic("write_to_rocksdb() failed reason = %s", err);

    rocksdb_writeoptions_destroy(writeoptions);
    rocksdb_writebatch_destroy(batch);

    // need to update rbuf_len and rbuf_s_index
    rock_w_lock();
    serverAssert(rbuf_len >= written);
    rbuf_len -= written;
    rbuf_s_index = index;
    rock_w_unlock();

    return written;
}

/*
 * The main entry for the thread of RocksDB write
 */
#define MIN_SLEEP_MICRO     16
#define MAX_SLEEP_MICRO     1024            // max sleep for 1 ms
static void* rock_write_main(void* arg)
{
    UNUSED(arg);

    int loop = 0;
    while (loop == 0)
        atomicGet(rock_threads_loop_forever, loop);
        
    unsigned int sleep_us = MIN_SLEEP_MICRO;
    while(loop)
    {        
        if (write_to_rocksdb() != 0)
        {
            sleep_us = MIN_SLEEP_MICRO;     // if task is coming, short the sleep time
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

/* Check whether the key is in ring buffer.
 * If not found, return -1. Otherise, the index in ring buffer. 
 *
 * The caller guarantees in lock mode.
 *
 * NOTE: We need check from the end of ring buffer, becuase 
 *       for duplicated keys in ring buf, the tail is newer than the head
 *       in the queue (i.e., ring buffer).
 */
static int exist_in_ring_buf_for_db_and_return_index(const int dbid, const sds redis_key)
{
    if (rbuf_len == 0)
        return -1;

    sds rock_key = sdsdup(redis_key);
    rock_key = encode_rock_key_for_db(dbid, rock_key);
    const size_t rock_key_len = sdslen(rock_key);

    int index = rbuf_e_index - 1;
    if (index == -1)
        index = RING_BUFFER_LEN - 1;

    for (int i = 0; i < rbuf_len; ++i)
    {
        if (rock_key_len == sdslen(rbuf_keys[index]) && sdscmp(rock_key, rbuf_keys[index]) == 0)
        {
            sdsfree(rock_key);
            return index;
        }

        --index;
        if (index == -1)
            index = RING_BUFFER_LEN - 1;
    }

    sdsfree(rock_key);
    return -1;
}

/* This is the API for rock_read.c. 
 * The caller guarantees not in lock mode of write.
 *
 * When a client needs recover some keys, it needs check ring buffer first.
 * The return is a list of recover vals (as sds) with same size as redis_keys (as same order).
 * 
 * If the key is in the ring buffer, the recover val (sds of serilized value) is duplicated.
 * Otherwise, recover val will be set to NULL. 
 * 
 * If no key in ring buf, the return list will be NULL. (and no resource allocated)
 * 
 * The caller needs to reclaim the resource if allocated.
 */
list* get_vals_from_write_ring_buf_first(const int dbid, const list *redis_keys)
{
    serverAssert(listLength(redis_keys) > 0);

    list *r = listCreate();
    int all_not_in_ring_buf = 1;

    rock_w_lock();

    listIter li;
    listNode *ln;
    listRewind((list*)redis_keys, &li);
    while ((ln = listNext(&li)))
    {
        sds redis_key = listNodeValue(ln);
        const int index = exist_in_ring_buf_for_db_and_return_index(dbid, redis_key);
        if (index == -1)
        {
            listAddNodeTail(r, NULL);
        }
        else
        {
            sds copy_val = sdsdup(rbuf_vals[index]);
            listAddNodeTail(r, copy_val);
            all_not_in_ring_buf = 0;
        }
    }

    rock_w_unlock();

    if (all_not_in_ring_buf)
    {
        // listSetFreeMethod(r, (void (*)(void*))sdsfree);
        listRelease(r);
        return NULL;
    }
    else
    {
        return r;
    }
}

void init_and_start_rock_write_thread()
{
    // Write spin lock must be inited before initiation of ring buffer
    init_write_spin_lock();

    init_write_ring_buffer();

    if (pthread_create(&rock_write_thread_id, NULL, rock_write_main, NULL) != 0) 
        serverPanic("Unable to create a rock write thread.");
}

