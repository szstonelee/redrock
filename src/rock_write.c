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

#include "rock_write.h"
#include "rock.h"
#include "rock_marshal.h"
#include "rock_read.h"
#include "rock_hash.h"
#include "rock_evict.h"
#include "rock_purge.h"

/* We use mutex to replace spinlock because spinlock could switch out 
 * by OS scheuler while holding lock and the other threads may be busy spiinlocking.
 * And for Linux, mutex uses futex for fast path which is similiar to spinlock.
 */
#ifdef RED_ROCK_MUTEX_DEBUG
static pthread_mutexattr_t mattr_write;
static pthread_mutex_t mutex_write;
#else
static pthread_mutex_t mutex_write = PTHREAD_MUTEX_INITIALIZER;     
#endif
static pthread_cond_t cv;

inline static void rock_w_lock() 
{
    serverAssert(pthread_mutex_lock(&mutex_write) == 0);
}
   
inline static void rock_w_unlock() 
{
    serverAssert(pthread_mutex_unlock(&mutex_write) == 0);
}

inline static void rock_w_wait_cond()
{
    serverAssert(pthread_cond_wait(&cv, &mutex_write) == 0);
}

void rock_w_signal_cond()
{
    serverAssert(pthread_cond_signal(&cv) == 0);
}

static pthread_t rock_write_thread_id;

/* -------------------------------------------
 * Ring Buffer as a write queue to RocksDB
 * -------------------------------------------
 */

static sds rbuf_keys[RING_BUFFER_LEN];
static sds rbuf_vals[RING_BUFFER_LEN];
// static int rbuf_invalids[RING_BUFFER_LEN];      // indicating whether the db is just emptied
static int rbuf_s_index;     // start index in queue (include if rbuf_len != 0)
static int rbuf_e_index;     // end index in queue (exclude if rbuf_len != 0)
static int rbuf_len;         // used(available) length

/* ------------------------------------------
 * Purge job waiting for write thread to write del to RocksDB
 * NOTE: the logic must guarantee the purge job is inserted before ring buffer job
 *       check rock_purge.c for the guarantee
 * ------------------------------------------
 */
static int del_db_dbids[ROCKSDB_PURGE_MAX_LEN];
static sds del_db_keys[ROCKSDB_PURGE_MAX_LEN];
static int del_hash_dbids[ROCKSDB_PURGE_MAX_LEN];
static sds del_hash_keys[ROCKSDB_PURGE_MAX_LEN];
static sds del_hash_fields[ROCKSDB_PURGE_MAX_LEN];

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

static void init_write_purge_data()
{
    rock_w_lock();
    del_db_keys[0] = NULL;
    del_hash_keys[0] = NULL;
    rock_w_unlock();
}

/* Called by Main thread in lock mode from caller.
 * keys and vals will be ownered by ring buffer 
 * so the caller can not use them anymore.
 * We free memory only when overwrite old and abandoned key/value
 */
static void batch_append_to_ringbuf(const int len, sds* keys, sds* vals) 
{
    for (int i = 0; i < len; ++i) 
    {
        const sds key = keys[i];
        const sds val = vals[i];
        serverAssert(key && val);

        if (rbuf_keys[rbuf_e_index]) 
        {
            // for the old and abandoned one, we releasse them
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
static int space_in_write_ring_buffer()
{
    rock_w_lock();
    const int space = RING_BUFFER_LEN - rbuf_len;
    rock_w_unlock();

    serverAssert(space >= 0 && space <= RING_BUFFER_LEN);
    return space;
}

/* Called by main thread to check whether there are no job of eviction
 * i.e., the ring buffer is empty
 * which is used for purge check.
 * Return true(1) if the ring buffer is empty, otherwise return false(0).
 */
int is_eviction_ring_buffer_empty()
{
    return space_in_write_ring_buffer() == RING_BUFFER_LEN;
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
 *        so all could be done in lock mode (but here not).
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
    batch_append_to_ringbuf(len, keys, vals);
    rock_w_signal_cond();
    rock_w_unlock();

    // release objs
    for (int i = 0; i < len; ++i)
    {
        decrRefCount(objs[i]);
    }
}

/* Called in main thread.
 * NOTE: The caller does not use keys, fields, vals anymore
 *       because keys will be encoded to rock_key in place and tranfer ownership to ring buffer,
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
    batch_append_to_ringbuf(len, keys, vals);
    rock_w_signal_cond();
    rock_w_unlock();

    // release fields
    for (int i = 0; i < len; ++i)
    {
        sdsfree(fields[i]);
    }
}


/* Called in main thread by cron and command ROCKEVICT.
 *
 * When caller select some keys (before setting rock value), it will call here
 * to determine which keys can be evicted to RocksDB 
 * 
 * and we need exclude those duplicated keys by the first key win with setting to rock value.
 * 
 * try_evict_to_rocksdb_for_db() will set the values to rock value for these matched keys.
 * 
 * Return the actual number of keys for eviction whkch are really setted to rock value
 *        and the estimated memory size for eviction because the eviction is for an robj.
 * 
 * NOTE1: The caller needs to use space_in_write_ring_buffer() first
 *        to know the available space for ring buffer.
 *        The caller needs to guarantee try_len <= space.
 * 
 * NOTE2: The keys could be duplicated 
 *         (only from command ROCKEVICT like rockevict key1 key1 but righ now does not use),
 *         we need to exclude the duplicated keys.
 * 
 * NOTE3: The keys must be not in rock hash 
 *        which means it can not be setted to rock value as a whole key.
 * 
 * NOTE4: The keys which are selected must not be in candidates.
 *        The caller needs exclude them from the try_keys and guarantee this.
 * 
 * NOTE5: The keys must be in redis db.
 */
#define OBJ_SIZE_SAMPLE_NUMBER      32
static int try_evict_to_rocksdb_for_db(const int try_len, const int *try_dbids, 
                                       const sds *try_keys, size_t *mem)
{
    size_t objectComputeSize(robj *o, size_t sample_size);  // declaration in object.c

    serverAssert(try_len > 0);

    if (mem)
        *mem = 0;

    int evict_len = 0;
    int evict_dbids[RING_BUFFER_LEN];
    sds evict_keys[RING_BUFFER_LEN];
    robj* evict_vals[RING_BUFFER_LEN];    
    for (int i = 0; i < try_len; ++i)
    {
        const int dbid = try_dbids[i];
        const sds try_key = try_keys[i];

        // check NOTE 3
        #ifdef RED_ROCK_DEBUG
        serverAssert(!is_in_rock_hash(dbid, try_key));
        #endif

        // the rock_read.c API. Keys in candidates means they are in async mode
        // and can removed from candidates later by main thread.
        // check NOTE 4.
        #ifdef RED_ROCK_DEBUG
        serverAssert(!already_in_candidates_for_db(dbid, try_key));  
        #endif

        // check NOTE 5.
        redisDb *db = server.db + dbid;
        dictEntry *de_db = dictFind(db->dict, try_key);
        serverAssert(de_db);
       
        robj *v = dictGetVal(de_db);
        if (!is_rock_value(v))
        {
            // the first one wins the setting rock value
            dictGetVal(de_db) = get_match_rock_value(v);
            on_rockval_key_for_rock_evict(dbid, dictGetKey(de_db));

            evict_dbids[evict_len] = dbid;
            // NOTE: we must duplicate tyr_key for write_batch_append_and_abandon()
            evict_keys[evict_len] = sdsdup(try_key);
            evict_vals[evict_len] = v;

            ++evict_len;
            if (mem)
                *mem += objectComputeSize(v, OBJ_SIZE_SAMPLE_NUMBER);
        }
    }

    serverAssert(evict_len > 0);

    write_batch_for_db_and_abandon(evict_len, evict_dbids, evict_keys, evict_vals);

    return evict_len;    
}

/* Called in main thread by server cron and command ROCKEVICTHASH
 *
 * When caller select some fields from some rock hashes, 
 * it will call here to determine which fields can be evicted to RocksDB 
 * 
 * and we also need to exclude those duplicatedd try_key + try_field (only poassible for ROCKEVICTHASH)
 * because it can not set to rock value twice
 * and the first key+field win the setting value to rock value.
 *
 * try_evict_to_rocksdb_for_hash() will set value to rock value for these matched fields.
 * Return the actual number of fields for eviction and the memory for eviction.
 *
 * NOTE1: The caller needs to use space_in_write_ring_buffer() first
 *        to know the available space for ring buffer.
 *        The caller needs to guarantee try_len <= space.
 *
 * NOTE2: The caller can not guarantee all the value can be evicted, 
 *        because the try_keys + try_fiels could be duplicated
 * 
 * NOTE3: The keys must be in rock hash.
 * 
 * NOTE4: The key+field which are selected must not be in candidates.
 *        The caller needs exclude them from the try_keys and guarantee this.
 * 
 * NOTE5: The key and filed must in redis db and has the correct type.
 */
static int try_evict_to_rocksdb_for_hash(const int try_len, const int *try_dbids, 
                                         const sds *try_keys, const sds *try_fields,
                                         size_t *mem)
{
    serverAssert(try_len > 0);

    if (mem)
        *mem = 0;

    int evict_len = 0;
    int evict_dbids[RING_BUFFER_LEN];
    sds evict_keys[RING_BUFFER_LEN];
    sds evict_fields[RING_BUFFER_LEN];
    sds evict_vals[RING_BUFFER_LEN];
    for (int i = 0; i < try_len; ++i)
    {
        const int dbid = try_dbids[i];
        const sds try_key = try_keys[i];
        const sds try_field = try_fields[i];

        // check NOTE 3
        #ifdef RED_ROCK_DEBUG
        serverAssert(is_in_rock_hash(dbid, try_key));
        #endif

        // the rock_read.c API and check NOTE 4
        #ifdef RED_ROCK_DEBUG
        serverAssert(!already_in_candidates_for_hash(dbid, try_key, try_field));
        #endif

        // check NOTE 5
        redisDb *db = server.db + dbid;
        dictEntry *de_db = dictFind(db->dict, try_key);
        serverAssert(de_db);
        robj *o = dictGetVal(de_db);
        serverAssert(!is_rock_value(o));
        serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT);
        dict *hash = o->ptr;
        dictEntry *de_hash = dictFind(hash, try_field);
        serverAssert(de_hash);

        sds v = dictGetVal(de_hash);
        if (v != shared.hash_rock_val_for_field)
        {
            // the first one wins the setting rock value
            dictGetVal(de_hash) = shared.hash_rock_val_for_field;
            on_rockval_field_of_hash(dbid, try_key, try_field);

            evict_dbids[evict_len] = dbid;
            // NOTE: we must duplicate for write_batch_for_hash_and_abandon()
            evict_keys[evict_len] = sdsdup(try_key);
            evict_fields[evict_len] = sdsdup(try_field);
            evict_vals[evict_len] = v;

            ++evict_len;
            if (mem)
                *mem += sdsAllocSize(v);
        }
    }

    serverAssert(evict_len > 0);

    write_batch_for_hash_and_abandon(evict_len, evict_dbids, evict_keys, evict_fields, evict_vals);

    return evict_len;
}

/* Called in main thread for command ROCKEVICT for db key.
 *
 * The caller need to guarantee:
 * 1. the key exists in DB and the type is OK
 * 2. the key is not rock value
 * 3. the key can not in candidates
 * 4. the key can not in rock hash
 *
 * If succesful, return TRY_EVICT_ONE_SUCCESS and if mem is valid, it saves the size of memory to evict.
 * 
 * If ring buffer is full right now, return TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL.
 * The caller can try again because the write thread is busy working 
 * and has not finished the writing jobs.
 * 
 */
int try_evict_one_key_to_rocksdb(const int dbid, const sds key, size_t *mem)
{
    const int space = space_in_write_ring_buffer();
    if (space == 0)
        return TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL;
    
    serverAssert(try_evict_to_rocksdb_for_db(1, &dbid, &key, mem) == 1);
    return TRY_EVICT_ONE_SUCCESS;
}

/* Called in main thread for command ROCKEVICTHASH for rock hash.
 * 
 * Check the above try_evict_one_key_to_rocksdb_by_rockevict_command() more help
 * because there are a lot of constraints.
 */
int try_evict_one_field_to_rocksdb(const int dbid, const sds key, 
                                   const sds field, size_t *mem)
{
    const int space = space_in_write_ring_buffer();
    if (space == 0)
        return TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL;

    serverAssert(try_evict_to_rocksdb_for_hash(1, &dbid, &key, &field, mem) == 1);
    return TRY_EVICT_ONE_SUCCESS;
}

/* Called by write thread to deal with purge task
 * The calller gurarantee:
 * 1. not in lock mode
 * 2. before dealing with ring buffer so there are no data race for RocksDB
 */
static void write_purge_to_rocksdb_first()
{
    // make lock as short as possible in write thread
    // by using local copy of sds pointers (i.e., the purge tasks)
    int db_dbids[ROCKSDB_PURGE_MAX_LEN];
    sds db_keys[ROCKSDB_PURGE_MAX_LEN];
    int hash_dbids[ROCKSDB_PURGE_MAX_LEN];
    sds hash_keys[ROCKSDB_PURGE_MAX_LEN];
    sds hash_fields[ROCKSDB_PURGE_MAX_LEN];

    rock_w_lock();

    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        db_dbids[i] = del_db_dbids[i];
        db_keys[i] = del_db_keys[i];

        if (del_db_keys[i] == NULL)
            break;        
    }

    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        hash_dbids[i] = del_hash_dbids[i];
        hash_keys[i] = del_hash_keys[i];
        hash_fields[i] = del_hash_fields[i];

        if (del_hash_keys[i] == NULL)
            break;
    }

    rock_w_unlock();

    if (db_keys[0] == NULL && hash_keys[0] == NULL)
        return;         // no purge task, just return

    // real write del to RocksDB (not in lock)
    rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(writeoptions, 1);      // disable WAL

    int del_cnt = 0;

    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        if (db_keys[i] == NULL)
            break;

        sds rock_key = sdsdup(db_keys[i]);      // must duplicate because db_keys[i] is immutable
        rock_key = encode_rock_key_for_db(db_dbids[i], rock_key);
        rocksdb_writebatch_delete(batch, rock_key, sdslen(rock_key));
        sdsfree(rock_key);
        ++del_cnt;
    }
    
    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        if (hash_keys[i] == NULL)
            break;

        sds rock_key = sdsdup(hash_keys[i]);
        rock_key = encode_rock_key_for_hash(hash_dbids[i], rock_key, hash_fields[i]);
        rocksdb_writebatch_delete(batch, rock_key, sdslen(rock_key));
        sdsfree(rock_key);
        ++del_cnt;
    }

    char *err = NULL;
    rocksdb_write(rockdb, writeoptions, batch, &err);    
    if (err) 
        serverPanic("write_purge_to_rocksdb_first() failed reason = %s", err);

    rocksdb_writeoptions_destroy(writeoptions);
    rocksdb_writebatch_destroy(batch);

    serverLog(LL_NOTICE, "purging partly %d key(or hash field) for RocksDB in background ...", del_cnt);

    // after write (del) to RocksDB, we need indicating the purge job finished
    rock_w_lock();
    del_db_keys[0] = NULL;
    del_hash_keys[0] = NULL;
    // for new candidates. 
    // NOTE: must be called in rock_w_lock() 
    //       otherwise main thread will do the transfer_purge_task_to_write_thread() two times
    // set_can_refressh_new_purge_candidates_to_true() use rock_p_lock
    // so hold rock_w_lock call for rock_p_lock
    // but do not tigger deadlock because only here have two locks holding (and in the same oreder)
    // We must guarantee has_unfinished_purge_task_for_write() and can_refresh_new_purge_candidates
    // to set atomic for do_purge_in_cron() in main thread
    set_can_refressh_new_purge_candidates_to_true();
    rock_w_unlock();
}

/* Called by write thread.
 * If nothing written to RocksDB, return 0. Otherwise, the number of key written to db.
 */
static int write_to_rocksdb()
{
    write_purge_to_rocksdb_first();     // must called before deal with ring buffer

    // Make lock as short as possible in write thread
    rock_w_lock();

    if (rbuf_len == 0)
    {
        rock_w_unlock();
        return 0;
    }
    int written = rbuf_len;
    int index = rbuf_s_index;

    // must check purge job again because it is async mode
    // e.g. main thread cron add some purge job when write thread sleep 
    //      after the first statement write_purge_to_rocksdb_first() in write_to_rocksdb()
    //      then main cron add some task to ring buffer and now write thread wake up 
    int has_purge_job = 0;
    if (!(del_db_keys[0] == NULL && del_hash_keys[0] == NULL))
        has_purge_job = 1;

    rock_w_unlock();

    if (has_purge_job)
        // this guaratee no more purge job comming when ring buffer has some jobs
        // check rock_purge.c do_purge_in_cron() which has is_eviction_ring_buffer_empty() checking
        write_purge_to_rocksdb_first();
       
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

/* When RedRock start, it may need to write the key and value dircectly to RocksDB
 * in the process of loading RDB or AOF
 * The caller guarantee in main thread and in init phase, i.e., no cron job in serverCron()
 */
void write_to_rocksdb_in_main_for_key_when_load(redisDb *db, const sds redis_key, const robj *redis_val)
{    
    sds rock_key = sdsdup(redis_key);
    rock_key = encode_rock_key_for_db(db->id, rock_key);
    sds rock_val = marshal_object(redis_val);

    rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(writeoptions, 1);      // disable WAL

    rocksdb_writebatch_put(batch, rock_key, sdslen(rock_key), rock_val, sdslen(rock_val));
    char *err = NULL;
    rocksdb_write(rockdb, writeoptions, batch, &err);    
    if (err) 
        serverPanic("write_to_rocksdb_in_main_for_key_when_load() failed reason = %s", err);

    rocksdb_writeoptions_destroy(writeoptions);
    rocksdb_writebatch_destroy(batch);

    sdsfree(rock_key);
    sdsfree(rock_val);
}

/* See above
 */
void write_to_rocksdb_in_main_for_hash_when_load(redisDb *db, const sds redis_key, const sds field, const sds field_val)
{
    sds rock_key = sdsdup(redis_key);
    rock_key = encode_rock_key_for_hash(db->id, rock_key, field);

    rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(writeoptions, 1);      // disable WAL

    rocksdb_writebatch_put(batch, rock_key, sdslen(rock_key), field_val, sdslen(field_val));
    char *err = NULL;
    rocksdb_write(rockdb, writeoptions, batch, &err);    
    if (err) 
        serverPanic("write_to_rocksdb_in_main_for_hash_when_load() failed reason = %s", err);

    rocksdb_writeoptions_destroy(writeoptions);
    rocksdb_writebatch_destroy(batch);

    sdsfree(rock_key);
}

/*
 * The main entry for the thread of RocksDB write
 */
// #define MIN_SLEEP_MICRO     16
// #define MAX_SLEEP_MICRO     1024            // max sleep for 1 ms
static void* rock_write_main(void* arg)
{
    UNUSED(arg);

    int loop = 0;
    while (loop == 0)
        atomicGet(rock_threads_loop_forever, loop);
        
    // unsigned int sleep_us = MIN_SLEEP_MICRO;
    while(loop)
    {
        rock_w_lock();

        while(loop && rbuf_len == 0 && (del_db_keys[0] == NULL && del_hash_keys[0] == NULL))
        {
            rock_w_wait_cond();
            atomicGet(rock_threads_loop_forever, loop);            
        } 

        rock_w_unlock();
        
        if (loop)
            write_to_rocksdb();
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

/* This is the API for rock rdb and aof.
 *
 * The caller guarantee not in lock mode and in redis process.
 * 
 * If found, return the sds value (duplicated), otherwise NULL.
 * The caller needs to release the resource if return is not NULL.
 */
sds get_key_val_str_from_write_ring_buf_first_in_redis_process(const int dbid, const sds key)
{
    sds val = NULL;

    rock_w_lock();
    const int index = exist_in_ring_buf_for_db_and_return_index(dbid, key);
    if (index != -1)
    {
        val = sdsdup(rbuf_vals[index]);
    }
    rock_w_unlock();

    return val;
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

/* This is the API for rock rdb and aof.
 *
 * The caller guarantee not in lock mode and in redis process.
 * 
 * If found, return the sds value (duplicated), otherwise NULL.
 * The caller needs to deal with the resource if return is not NULL.
 */
sds get_field_val_str_from_write_ring_buf_first_in_redis_process(const int dbid, const sds hash_key, const sds field)
{
    sds val = NULL;

    rock_w_lock();
    const int index = exist_in_ring_buf_for_hash_and_return_index(dbid, hash_key, field);
    if (index != -1)
    {
        val = sdsdup(rbuf_vals[index]); 
    }
    rock_w_unlock();

    return val;
}

/* Called in main thread */
void init_and_start_rock_write_thread()
{
#ifdef RED_ROCK_MUTEX_DEBUG
    serverAssert(pthread_mutexattr_init(&mattr_write) == 0);
    serverAssert(pthread_mutexattr_settype(&mattr_write, PTHREAD_MUTEX_ERRORCHECK) == 0);
    serverAssert(pthread_mutex_init(&mutex_write, &mattr_write) == 0);
#endif
    serverAssert(pthread_cond_init(&cv, NULL) == 0);

    init_write_ring_buffer();
    init_write_purge_data();

    if (pthread_create(&rock_write_thread_id, NULL, rock_write_main, NULL) != 0) 
        serverPanic("Unable to create a rock write thread.");
}

/* Called in main thread for rock.c when main thread will exit */
void join_write_thread()
{
    int s;
    void *res;

    s = pthread_join(rock_write_thread_id, &res);
    if (s != 0)
    {
        serverLog(LL_WARNING, "rock write thread join failure! err = %d", s);
    }
    else
    {
        serverLog(LL_NOTICE, "rock write thread exit and join successfully.");
    }
}

/* Create a snapshot for ring buffer for child process.
 * 
 * NOTE:
 * 1. We use write lock to guarantee the integrity of ring buffer
 * 2. We duplicate the keys and vals in ring buffer to the output of keys and vals,
 *    so the caller needs to reclaim the resource
 * 3. After return (and unlcok), the caller can use a snapshot for RocksDB,
 *    bcause everything check the duplicated keys and vals first, 
 *    so the snapshot of RocksDB can have newer dataset which maybe from ring buffer after unlock.
 * 4. unlike ring buffer, the newer data is lower index in keys
 * 5. the caller guarantee that keys and vals must have the same size of RING_BUFFER_LEN
 */
void create_snapshot_of_ring_buf_for_child_process(sds *keys, sds *vals)
{
    rock_w_lock();

    int index = rbuf_e_index - 1;
    if (index == -1)
        index = RING_BUFFER_LEN - 1;
    int len = rbuf_len;

    for (int i = 0; i < RING_BUFFER_LEN; ++i)
    {
        serverAssert(keys[i] == NULL && vals[i] == NULL);

        if (len != 0)
        {
            serverAssert(rbuf_keys[index] != NULL && rbuf_vals[index] != NULL);
                
            keys[i] = sdsdup(rbuf_keys[index]);
            vals[i] = sdsdup(rbuf_vals[index]);

            --len;
            --index;
            if (index == 0)
                index = RING_BUFFER_LEN - 1;
        }
    }

    rock_w_unlock();
}

/* Called by main thread in cron to check whether the purge task has been finished
 * return true (1) if un-finished
 * otherwise return 0.
 */
int has_unfinished_purge_task_for_write()
{
    int no_job = 0;
    
    rock_w_lock();
    if (del_db_keys[0] == NULL && del_hash_keys[0] == NULL)
        no_job = 1;
    rock_w_unlock();

    return !no_job;
}

/* Called by main thread in cron to add purge task to write thead
 */
void transfer_purge_task_to_write_thread(int db_cnt, int *db_dbids, sds *db_keys,
                                         int hash_cnt, int *hash_dbids, sds *hash_keys, sds *hash_fields)
{
    serverAssert(db_cnt > 0 || hash_cnt > 0);

    rock_w_lock();

    serverAssert(del_db_keys[0] == NULL && del_hash_keys[0] == NULL);

    for (int i = 0; i < db_cnt; ++i)
    {
        del_db_dbids[i] = db_dbids[i];
        del_db_keys[i] = db_keys[i];
    }

    if (db_cnt != ROCKSDB_PURGE_MAX_LEN)
        del_db_keys[db_cnt] = NULL;

    for (int i = 0; i < hash_cnt; ++i)
    {
        del_hash_dbids[i] = hash_dbids[i];
        del_hash_keys[i] = hash_keys[i];
        del_hash_fields[i] = hash_fields[i];
    }

    if (hash_cnt != ROCKSDB_PURGE_MAX_LEN)
        del_hash_keys[hash_cnt] = NULL;

    rock_w_signal_cond();   // wake up write thread to do purge task

    rock_w_unlock();
}

/* Called by  main thread cron when do purge check
 */
void try_to_wakeup_write_thread()
{
    rock_w_lock();
    rock_w_signal_cond();
    rock_w_unlock();
}
