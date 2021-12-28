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
// static int rbuf_invalids[RING_BUFFER_LEN];      // indicating whether the db is just emptied
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
        // rbuf_invalids[i] = 0;
    }
    rbuf_s_index = rbuf_e_index = 0;
    rbuf_len = 0;
    rock_w_unlock();
}

/* Called by Main thread in lock mode from caller.
 * keys and vals will be ownered by ring buffer 
 * so the caller can not use them anymore.
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
        // rbuf_invalids[rbuf_e_index] = 0;        // must set 0 to overwirte possible 1 for the previous used index

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
 * NOTE2: Need a deep think??? I move out the marshal code out of lock mode.
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
static void write_batch_for_db_and_abandon(const int len, const int *dbids, sds *keys, robj **objs)
{
    sds vals[RING_BUFFER_LEN];
    for (int i = 0; i < len; ++i)
    {
        sds rock_key = encode_rock_key_for_db(dbids[i], keys[i]);
        keys[i] = rock_key;
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

/* Called in main thread.
 * NOTE: The caller does not use keys, fields, vals anymore
 *       because keys will changed to rock_key and tranfer ownership to ring buffer,
 *       vals will be transfered ownership to ring buffer.
 *       and fields will be reclaimed here.
 */
static void write_batch_for_hash_and_abandon(const int len, const int *dbids, sds *keys, sds *fields, sds *vals)
{
    for (int i = 0; i < len; ++i)
    {        
        sds rock_key = encode_rock_key_for_hash(dbids[i], keys[i], fields[i]);
        keys[i] = rock_key;
    }

    rock_w_lock();
    serverAssert(rbuf_len + len <= RING_BUFFER_LEN);
#ifdef RED_ROCK_DEBUG
    serverAssert(debug_check_no_candidates(len, keys));
#endif
    batch_append_to_ringbuf(len, keys, vals);
    rock_w_unlock();

    // release fields
    for (int i = 0; i < len; ++i)
    {
        sdsfree(fields[i]);
    }
}

/* Called in main thread cron and command ROCKEVICT.
 *
 * When cron() select some keys (before setting rock value), it will call here
 * to determine which keys can be evicted to RocksDB 
 * because we need exclude those keys in read_rock_key_candidates. 
 * Check rock_read.c for read_rock_key_candidates for more reference. 
 * 
 * and we need exclude those duplicated keys.
 * 
 * try_evict_to_rocksdb_for_db() will set value to rock value for these matched keys.
 * Return the actual number of keys for eviction.
 * 
 * NOTE1: The caller needs to use space_in_write_ring_buffer() first
 *        to know the available space for ring buffer.
 *        The caller needs to guarantee check_len <= space.
 * 
 * NOTE2: The keys could be duplicated (from command ROCKEVICT),
 *         we need to exclude the duplicated keys.
 * 
 * NOTE3: The keys must be not in rock hash.
 */
int try_evict_to_rocksdb_for_db(const int try_len, const int *try_dbids, const sds *try_keys)
{
    serverAssert(try_len > 0);

    // first exclude those in candidates
    int exclude_candidate_len = 0;
    int exclude_candidate_dbids[RING_BUFFER_LEN];
    sds exclude_candidate_keys[RING_BUFFER_LEN];    
    for (int i = 0; i < try_len; ++i)
    {
        const int dbid = try_dbids[i];
        const sds try_key = try_keys[i];

        serverAssert(!is_in_rock_hash(dbid, try_key));

        if (already_in_candidates_for_db(dbid, try_key))     // the rock_read.c API
            continue;

        // NOTE: we must duplicate for write_batch_append_and_abandon()
        exclude_candidate_keys[exclude_candidate_len] = sdsdup(try_key);  
        exclude_candidate_dbids[exclude_candidate_len] = dbid;
        ++exclude_candidate_len;
    }

    if (exclude_candidate_len == 0)
        return 0;

    // second we need exclude those duplicated key+field
    int evict_len = 0;
    int evict_dbids[RING_BUFFER_LEN];
    sds evict_keys[RING_BUFFER_LEN];
    robj* evict_vals[RING_BUFFER_LEN];
    for (int i = 0; i < exclude_candidate_len; ++i)
    {
        int dbid = exclude_candidate_dbids[i];
        sds redis_key = exclude_candidate_keys[i];

        redisDb *db = server.db + dbid;
        dictEntry *de_db = dictFind(db->dict, redis_key);
        serverAssert(de_db);

        robj *v = dictGetVal(de_db);
        if (is_evict_value(v))
        {
            dictGetVal(de_db) = get_match_rock_value(v);

            evict_dbids[evict_len] = dbid;
            evict_keys[evict_len] = redis_key;
            evict_vals[evict_len] = v;
            ++evict_len;
        }
        else
        {
            sdsfree(redis_key);
        }
    }

    if (evict_len)    
        write_batch_for_db_and_abandon(evict_len, evict_dbids, evict_keys, evict_vals);

    return evict_len;    
}

/* Called in main thread cron and command ROCKEVICTHASH
 *
 * When cron() select some fields from some hashes (before setting rock value), it will call here
 * to determine which fields can be evicted to RocksDB 
 * because we need exclude those keys in read_rock_key_candidates. 
 * Check rock_read.c for read_rock_key_candidates for more reference. 
 * 
 * and we also need to exclude those duplicatedd try_key + try_field 
 * because it can not set to rock value twice.
 *
 * try_evict_to_rocksdb_for_hash() will set value to rock value for these matched fields.
 * Return the actual number of fields for eviction.
 *
 * NOTE1: The caller needs to use space_in_write_ring_buffer() first
 *        to know the available space for ring buffer.
 *        The caller needs to guarantee check_len <= space.
 *
 * NOTE2: The caller can not guarantee all the value can be evicted, 
 *        because the try_keys + try_fiels could be duplicated
 */
int try_evict_to_rocksdb_for_hash(const int try_len, const int *try_dbids, const sds *try_keys, const sds *try_fields)
{
    serverAssert(try_len > 0);

    // first exclude those in candidates
    int exclude_candidate_len = 0;
    int exclude_candidate_dbids[RING_BUFFER_LEN];
    sds exclude_candidate_keys[RING_BUFFER_LEN];
    sds exclude_candidate_fields[RING_BUFFER_LEN];
    for (int i = 0; i < try_len; ++i)
    {
        const int dbid = try_dbids[i];
        const sds try_key = try_keys[i];
        const sds try_field = try_fields[i];

        serverAssert(is_in_rock_hash(dbid, try_key));

        if (already_in_candidates_for_hash(dbid, try_key, try_field))     // the rock_read.c API
            continue;

        // NOTE: we must duplicate for write_batch_append_and_abandon()
        exclude_candidate_keys[exclude_candidate_len] = sdsdup(try_key);
        exclude_candidate_fields[exclude_candidate_len] = sdsdup(try_field);  
        exclude_candidate_dbids[exclude_candidate_len] = dbid;
        ++exclude_candidate_len; 
    }

    if (exclude_candidate_len == 0)
        return 0;
        
    // second we need exclude those duplicated key+field
    int evict_len = 0;
    int evict_dbids[RING_BUFFER_LEN];
    sds evict_keys[RING_BUFFER_LEN];
    sds evict_fields[RING_BUFFER_LEN];
    sds evict_vals[RING_BUFFER_LEN];
    for (int i = 0; i < exclude_candidate_len; ++i)
    {
        int dbid = exclude_candidate_dbids[i];
        sds hash_key = exclude_candidate_keys[i];
        sds hash_field = exclude_candidate_fields[i];

        redisDb *db = server.db + dbid;
        dictEntry *de_db = dictFind(db->dict, hash_key);
        serverAssert(de_db);
        robj *o = dictGetVal(de_db);
        serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);
        dict *hash = o->ptr;
        dictEntry *de_hash = dictFind(hash, hash_field);
        serverAssert(de_hash);

        sds v = dictGetVal(de_hash);
        if (v != shared.hash_rock_val_for_field)
        {
            dictGetVal(de_hash) = shared.hash_rock_val_for_field;
            on_rockval_field_of_hash(dbid, hash_key, hash_field);

            evict_dbids[evict_len] = dbid;
            evict_keys[evict_len] = hash_key;
            evict_fields[evict_len] = hash_field;
            evict_vals[evict_len] = v;
            ++evict_len;
        }
        else
        {
            sdsfree(hash_key);
            sdsfree(hash_field);
        }
    }

    if (evict_len)
        write_batch_for_hash_and_abandon(evict_len, evict_dbids, evict_keys, evict_fields, evict_vals);

    return evict_len;
}

/* Called in main thread for command ROCKEVICT for db key.
 *
 * If succesful, return TRY_EVICT_ONE_KEY_SUCCESS.
 * 
 * If ring buffer is full right now, return TRY_EVICT_ONE_KEY_RING_BUFFER_FULL.
 * The caller can try again because the write thread is working 
 * and has not finished the wrintg jobs.
 * 
 * Otherwise return TRY_EVICT_ONE_KEY_FAIL_FOR_IN_CANDIDATES.
 * The caller can not try again because main thread must return to event loop
 * to let it get the read signal from read thread 
 * which will deal with candidates by deleting task. 
 * 
 * The caller needs to guarantee the value in Redis exist and can be evicted to RocksDB.
 */
int try_evict_one_key_to_rocksdb(const int dbid, const sds key)
{
    const int space = space_in_write_ring_buffer();
    if (space == 0)
        return TRY_EVICT_ONE_KEY_RING_BUFFER_FULL;
    
    if (try_evict_to_rocksdb_for_db(1, &dbid, &key) == 0)
    {
        return TRY_EVICT_ONE_KEY_FAIL_FOR_IN_CANDIDATES_OR_ALREADY_ROCK_VALUE;
    }
    else
    {
        return TRY_EVICT_ONE_KEY_SUCCESS;
    }
}

/* Called in main thread for command ROCKEVICTHASH for rock hash.
 * 
 * Check the above try_evict_one_key_to_rocksdb() help.
 */
int try_evict_one_field_to_rocksdb(const int dbid, const sds key, const sds field)
{
    const int space = space_in_write_ring_buffer();
    if (space == 0)
        return TRY_EVICT_ONE_KEY_RING_BUFFER_FULL;


    if (try_evict_to_rocksdb_for_hash(1, &dbid, &key, &field) == 0)
    {
        return TRY_EVICT_ONE_KEY_FAIL_FOR_IN_CANDIDATES_OR_ALREADY_ROCK_VALUE;
    }
    else
    {
        return TRY_EVICT_ONE_KEY_SUCCESS;
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
       
    // for manual debug
    // serverLog(LL_WARNING, "write thread write rocksdb start (sleep for 10 seconds) ...");
    // sleep(10);
    // serverLog(LL_WARNING, "write thread write rocksdb end!!!!!");

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

/* Called in main thread.
 *
 * Check whether the key is in ring buffer.
 * If not found, return -1. Otherise, the index in ring buffer. 
 *
 * The caller guarantees in lock mode.
 *
 * NOTE1: We need check from the end of ring buffer, becuase 
 *        for duplicated keys in ring buf, the tail is newer than the head
 *        in the queue (i.e., ring buffer).
 * 
 * NOTE2: If the invalid of the key is true, it means the db has just been emptied,
 *        we can not use it, and we do not need to advance for any more. 
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
            // return rbuf_invalids[index] ? -1 : index;
            return index;
        }

        --index;
        if (index == -1)
            index = RING_BUFFER_LEN - 1;
    }

    sdsfree(rock_key);
    return -1;
}

/* Called in main thead.
 *
 * Check whether the key and field is in ring buffer.
 * If not found, return -1. Otherise, the index in ring buffer. 
 *
 * The caller guarantees in lock mode.
 *
 * NOTE: We need check from the end of ring buffer, becuase 
 *       for duplicated keys in ring buf, the tail is newer than the head
 *       in the queue (i.e., ring buffer).
 */
static int exist_in_ring_buf_for_hash_and_return_index(const int dbid, const sds redis_key, const sds field)
{
    if (rbuf_len == 0)
        return -1;

    sds rock_key = sdsdup(redis_key);
    rock_key = encode_rock_key_for_hash(dbid, rock_key, field);
    const size_t rock_key_len = sdslen(rock_key);

    int index = rbuf_e_index - 1;
    if (index == -1)
        index = RING_BUFFER_LEN - 1;

    for (int i = 0; i < rbuf_len; ++i)
    {
        if (rock_key_len == sdslen(rbuf_keys[index]) && sdscmp(rock_key, rbuf_keys[index]) == 0)
        {
            sdsfree(rock_key);
            // return rbuf_invalids[index] ? -1 : index;
            return index;
        }

        --index;
        if (index == -1)
            index = RING_BUFFER_LEN - 1;
    }

    sdsfree(rock_key);
    return -1;
}

/* Called in main thread.
 *
 * This is the API for rock_read.c. 
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
list* get_vals_from_write_ring_buf_first_for_db(const int dbid, const list *redis_keys)
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
        listRelease(r);
        return NULL;
    }
    else
    {
        return r;
    }
}

/* Called in main thread.
 *
 * This is the API for rock_read.c. 
 * The caller guarantees not in lock mode of write.
 *
 * When a client needs recover some hash keys with field, it needs check ring buffer first.
 * The return is a list of recover vals (as sds) with same size as hash_keys (as same order).
 * 
 * If the key is in the ring buffer, the recover val is duplicated.
 * Otherwise, recover val will be set to NULL. 
 * 
 * If no key in ring buf, the return list will be NULL. (and no resource allocated)
 * 
 * The caller needs to reclaim the resource if allocated.
 */
list* get_vals_from_write_ring_buf_first_for_hash(const int dbid, const list *hash_keys, const list *fields)
{
    serverAssert(listLength(hash_keys) > 0);
    serverAssert(listLength(hash_keys) == listLength(fields));

    list *r = listCreate();
    int all_not_in_ring_buf = 1;

    rock_w_lock();

    listIter li_key;
    listNode *ln_key;
    listRewind((list*)hash_keys, &li_key);
    listIter li_field;
    listNode *ln_field;
    listRewind((list*)fields, &li_field);
    while ((ln_key = listNext(&li_key)))
    {
        ln_field = listNext(&li_field);

        sds hash_key = listNodeValue(ln_key);
        sds field = listNodeValue(ln_field);
        const int index = exist_in_ring_buf_for_hash_and_return_index(dbid, hash_key, field);
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
        listRelease(r);
        return NULL;
    }
    else
    {
        return r;
    }
}

/* Called in main thread */
void init_and_start_rock_write_thread()
{
    // Write spin lock must be inited before initiation of ring buffer
    init_write_spin_lock();

    init_write_ring_buffer();

    if (pthread_create(&rock_write_thread_id, NULL, rock_write_main, NULL) != 0) 
        serverPanic("Unable to create a rock write thread.");
}

