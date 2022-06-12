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


/* Purge thread do two thing:
 * 1. Some stat info related to RocksDB, disk size of all SST files, estimated key number
 * 2. If purgetrocksdb command is issued, do the purge job in background
 */

#include "rock_purge.h"
#include "rock.h"
#include "rock_write.h"

#ifdef RED_ROCK_MUTEX_DEBUG
static pthread_mutexattr_t mattr_purge;
static pthread_mutex_t mutex_purge;
#else
static pthread_mutex_t mutex_purge = PTHREAD_MUTEX_INITIALIZER;     
#endif

inline static void rock_p_lock() 
{
    serverAssert(pthread_mutex_lock(&mutex_purge) == 0);
}
   
inline static void rock_p_unlock() 
{
    serverAssert(pthread_mutex_unlock(&mutex_purge) == 0);
}

static pthread_t rock_purge_thread_id;

/* -----------------------------------
 * Define the data for rocksdb purge 
 * -----------------------------------
 */
static int can_refresh_new_purge_candidates;
static int purge_db_dbids[ROCKSDB_PURGE_MAX_LEN];
static sds purge_db_key_candidates[ROCKSDB_PURGE_MAX_LEN];
static int purge_hash_dbids[ROCKSDB_PURGE_MAX_LEN];
static sds purge_hash_key_candidates[ROCKSDB_PURGE_MAX_LEN];
static sds purge_hash_field_candidates[ROCKSDB_PURGE_MAX_LEN];
static rocksdb_iterator_t *rocksdb_it;

/* ------------------------------------
 * Define the Rock stat for RocksDB
 * ------------------------------------
 */
static size_t rocksdb_sst_size = 0;
static size_t estimate_rocksdb_key_num = 0;

/* Call RocksDB property to get total SST files size
 * Return SIZE_MAX meaning it fails.
 */
static size_t get_rocksdb_disk_size()
{
    uint64_t disk_sz;

    const int res = rocksdb_property_int(rockdb, "rocksdb.total-sst-files-size", &disk_sz);

    if (res != 0)
    {
        serverLog(LL_WARNING, "get_rocksdb_disk_size() failed!");
        return SIZE_MAX;
    }

    return disk_sz;
}

/* Call RocksDB property to get estimate key number.
 * Return SIZE_MAX meaning it fails.
 */
static size_t get_rocksdb_estimate_key_num()
{
    uint64_t key_num;

    const int res = rocksdb_property_int(rockdb, "rocksdb.estimate-num-keys", &key_num);

    if (res != 0)
    {
        serverLog(LL_WARNING, "get_rocksdb_estimated_key_num() failed");
        return SIZE_MAX;
    }

    return key_num;
}

/* API for main thread to get the up-to-date(in seconds) estimated db size and key num in RocksDB 
 */
void get_refresh_rocksdb_db_size_and_key_num(size_t *disk_size, size_t *key_num)
{
    rock_p_lock();
    *disk_size = rocksdb_sst_size;
    *key_num = estimate_rocksdb_key_num;
    rock_p_unlock();
}

/* Refresh the stat for RocksDB (disk size and key number) every second
 * in main thread cron job
 */
void update_rocksdb_stat_in_cron()
{
    static int cron_cnt = 0;

    ++cron_cnt;
    if (cron_cnt != server.hz)
        return;
    
    cron_cnt = 0;

    size_t disk_size, key_num;
    get_refresh_rocksdb_db_size_and_key_num(&disk_size, &key_num);
    server.rocksdb_disk_size = disk_size;
    server.rocksdb_key_num = key_num;
}

static void refresh_rocksdb_stat()
{
    size_t disk_sz = get_rocksdb_disk_size();
    size_t key_num = get_rocksdb_estimate_key_num();
            
    if (!(disk_sz == SIZE_MAX && key_num == SIZE_MAX))
    {
        rock_p_lock();

        if (disk_sz != SIZE_MAX)
            rocksdb_sst_size = disk_sz;

        if (key_num != SIZE_MAX)
            estimate_rocksdb_key_num = key_num;

        rock_p_unlock();
    }
}

/* Called by main thread and write thread
 *
 * When main thread finishs all candidates and finds no need to write to RocksDB,
 * it will call here.
 * 
 * When write thread finish writing DELEte keys, it will call here.
 */
void set_can_refressh_new_purge_candidates_to_true()
{
    rock_p_lock();
    serverAssert(!can_refresh_new_purge_candidates);
    can_refresh_new_purge_candidates = 1;       // set TRUE so new refresh can happen
    rock_p_unlock();
}

/* This is called in purge thread。
 * The caller guarantee not in lock mode.
 */
static void reclaim_old_candidates()
{
    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        if (purge_db_key_candidates[i] == NULL)
            break;
        
        sdsfree(purge_db_key_candidates[i]);
        purge_db_key_candidates[i] = NULL;
    }

    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        if (purge_hash_key_candidates[i] == NULL)
            break;
        
        sdsfree(purge_hash_key_candidates[i]);
        purge_hash_key_candidates[i] = NULL;
        serverAssert(purge_hash_field_candidates[i]);
        sdsfree(purge_hash_field_candidates[i]);
        purge_hash_field_candidates[i] = NULL;
    }
}

static void check_rocksdb_iterator_error(const char *from)
{
    char *err_check = NULL;
    rocksdb_iter_get_error(rocksdb_it, &err_check);

    if (err_check == NULL)
        return;
    
    serverLog(LL_WARNING, "check_rocksdb_iterator_error() failed, from = %s, err_check = %s", from , err_check);
    exit(1); 
}

/* For refresh candidatess, we use lock in a smart way.
 * For allocation of new candidates, we use no lock and local copy for candidates (only pointer)
 * Then use lock and copy locals to candidates which can been seen by other threads (main thread and write thread)
 * Because other threads do not change the sds of candidates, we can guarantee the data race.
 */
static void refresh_candidates(sds *last_rock_key)
{
    serverAssert(rocksdb_it && rocksdb_iter_valid(rocksdb_it));

    // when allocate, we use local array, 
    // i.e.,  rock_keys & db_keys & hash_keys & hash_fields for avoid lock 
    sds rock_keys[ROCKSDB_PURGE_MAX_LEN];
    int last_index = -1;
    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        if (!rocksdb_iter_valid(rocksdb_it))
        {
            check_rocksdb_iterator_error("refresh_candidates() call rocksdb_iter_valid()");
            break;
        }
        
        last_index = i;

        size_t rock_key_len;
        const char *rock_key_buf = rocksdb_iter_key(rocksdb_it, &rock_key_len);
        serverAssert(rock_key_buf != NULL);
        rock_keys[i] = sdsnewlen(rock_key_buf, rock_key_len);

        rocksdb_iter_next(rocksdb_it);
    }

    serverAssert(last_index != -1);

    // deal with last rock key
    sdsfree(*last_rock_key);
    *last_rock_key = sdsdup(rock_keys[last_index]);     // duplicate for last_rock_key because rock_keys will be freed

    // decode rock keys for db and hash    
    int db_dbids[ROCKSDB_PURGE_MAX_LEN];
    sds db_keys[ROCKSDB_PURGE_MAX_LEN];
    int hash_dbids[ROCKSDB_PURGE_MAX_LEN];
    sds hash_keys[ROCKSDB_PURGE_MAX_LEN];
    sds hash_fields[ROCKSDB_PURGE_MAX_LEN];
    int db_cnt = 0;
    int hash_cnt = 0;

    for (int i = 0; i <= last_index; ++i)
    {
        sds rock_key = rock_keys[i];
        serverAssert(sdslen(rock_key) > 0);

        if (rock_key[0] == ROCK_KEY_FOR_DB)
        {
            int dbid;
            const char *redis_key;
            size_t key_sz;
            decode_rock_key_for_db(rock_key, &dbid, &redis_key, &key_sz);
            db_dbids[db_cnt] = dbid;
            db_keys[db_cnt] = sdsnewlen(redis_key, key_sz);
            ++db_cnt;
        }
        else
        {
            serverAssert(rock_key[0] == ROCK_KEY_FOR_HASH);
            int dbid;
            const char *hash_key;
            size_t key_sz;
            const char *hash_field;
            size_t field_sz;
            decode_rock_key_for_hash(rock_key, &dbid, &hash_key, &key_sz, &hash_field, &field_sz);
            hash_dbids[hash_cnt] = dbid;
            hash_keys[hash_cnt] = sdsnewlen(hash_key, key_sz);
            hash_fields[hash_cnt] = sdsnewlen(hash_field, field_sz);
            ++hash_cnt;
        }
    }

    // deallocate rock_keys
    for (int i = 0; i <= last_index; ++i)
    {
        sdsfree(rock_keys[i]);
        rock_keys[i] = NULL;
    }

    // use lock for purge data (considering data race)
    rock_p_lock();    
    for (int i = 0; i < db_cnt; ++i)
    {
        serverAssert(purge_db_key_candidates[i] == NULL);
        purge_db_dbids[i] = db_dbids[i];
        purge_db_key_candidates[i] = db_keys[i];
    }
    // this make other threads can see the end of purge_candidates
    // because reclaim_old_candidates() called with no lock
    if (db_cnt != ROCKSDB_PURGE_MAX_LEN)
    {
        purge_db_key_candidates[db_cnt] = NULL;
    }

    for (int i = 0; i < hash_cnt; ++i)
    {
        serverAssert(purge_hash_key_candidates[i] == NULL);
        purge_hash_dbids[i] = hash_dbids[i];
        purge_hash_key_candidates[i] = hash_keys[i];
        purge_hash_field_candidates[i] = hash_fields[i];
    }
    if (hash_cnt != ROCKSDB_PURGE_MAX_LEN)
    {
        purge_hash_key_candidates[hash_cnt] = NULL;
        purge_hash_field_candidates[hash_cnt] = NULL;
    }
    rock_p_unlock();
}


/* Check rocksdb purge working state in main thread for command ROCKSDBPURGE
 */
int get_rocksdb_purge_working_state()
{
    rock_p_lock();
    const int state = server.rocksdb_purge_working;
    rock_p_unlock();

    return state;
}

/*
void debug_print_rock_key(const char *p, const size_t l, const char *caller)
{
    if (p[0] != ROCK_KEY_FOR_DB)
    {
        serverLog(LL_WARNING, "caller = %s, not string rock key", caller);
        return;
    }

    if (l < 2)
    {
        serverLog(LL_WARNING, "caller = %s, l = %zu", caller, l);
        return;
    }

    sds key = sdsnewlen(p+2, l-2);
    serverLog(LL_WARNING, "caller = %s, key = %s", caller, key);
    sdsfree(key);
}
*/

/* because RocksDB C API does not support iterator::refresh() 
 * We use a similiar algorithm by
 * for a period (1 minute or 5 minutes) 
 * we release iterator and 
 * build it again and locate the iterator to the next of last_refresh_rock_key
 */
static void change_snapshot_for_iterator(sds last_refresh_rock_key)
{
    serverAssert(rocksdb_it != NULL);
    serverAssert(last_refresh_rock_key != NULL);

    // release old snapshot by abandon iterator    
    rocksdb_iter_destroy(rocksdb_it);   
    
    // change to current snapshot
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    serverAssert(rocksdb_it = rocksdb_create_iterator(rockdb, readoptions));     
    check_rocksdb_iterator_error("change_snapshot_for_iterator() call rocksdb_create_iterator for change snapshot");
    rocksdb_readoptions_destroy(readoptions);

    // start from the next key of last_refresh_rock_key 
    // NOTE: last_refresh_rock_key could not exist because snapshot changed
    // debug_print_rock_key(last_refresh_rock_key, sdslen(last_refresh_rock_key), "last_refresh_rock_key");
    rocksdb_iter_seek_for_prev(rocksdb_it, last_refresh_rock_key, sdslen(last_refresh_rock_key));
    check_rocksdb_iterator_error("change_snapshot_for_iterator() call rocksdb_iter_seek_for_prev()");    
    if (rocksdb_iter_valid(rocksdb_it))
    {
        rocksdb_iter_next(rocksdb_it);              
        check_rocksdb_iterator_error("change_snapshot_for_iterator() call rocksdb_iter_next()");

        /*
        if (rocksdb_iter_valid(rocksdb_it))
        {
            serverLog(LL_WARNING, "rocksdb_iter_next() is valid");
            size_t len;
            const char *p = rocksdb_iter_key(rocksdb_it, &len);
            debug_print_rock_key(p, len, "next_key_is_valid");
        }
        else
        {
            serverLog(LL_WARNING, "next key is not valid!");
        }
        */
    }
    else
    {
        // If last_refresh_rock_key has been deleted by write thread, it is possible 
        // last_refresh_rock_key is just before the head of RocksDB of all remain keys
        // This time rocksdb_iter_seek_for_prev() return not valid iteerator
        // so we need start from the head of RocksDB
        // check https://hub.fastgit.xyz/facebook/rocksdb/blob/main/include/rocksdb/iterator.h
        // for Iterator::SeekForPrev()
        rocksdb_iter_seek_to_first(rocksdb_it);
        check_rocksdb_iterator_error("change_snapshot_for_iterator() call rocksdb_iter_seek_to_first()");
        // serverLog(LL_WARNING, "rocksdb_iter_seek_for_prev() not valid!");
    }
}

static void init_iterator()
{
    serverAssert(rocksdb_it == NULL);

    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    serverAssert(rocksdb_it = rocksdb_create_iterator(rockdb, readoptions));
    check_rocksdb_iterator_error("do_purge() call rocksdb_create_iterator");
    rocksdb_readoptions_destroy(readoptions);

    rocksdb_iter_seek_to_first(rocksdb_it);
    check_rocksdb_iterator_error("do_purge() call rocksdb_iter_seek_to_first");
}

/* This is called in purge thread for every second
 */
static void do_purge()
{
    // check the server state (modified by ROCKSDBPURGE command) 
    // which allow the purge background job
    if (!get_rocksdb_purge_working_state())
        return; 

    // we need purge    

    // check whether can refresh the candidates
    // i.e., whenter the old candidates have been processed
    static sds last_refresh_rock_key = NULL;
    static int low_priority_cnt = 0;
    static int refresh_cnt = 0;

    ++low_priority_cnt;
    rock_p_lock();
    int need_refresh = can_refresh_new_purge_candidates;
    rock_p_unlock();
    if (!need_refresh)
    {
        // main thread or write thread is busy for other thing. 
        // Candidates are fresh and need to be hold right now.
        if (low_priority_cnt == 300)
        {
            // because main thread could be busy do eviction job as purge job has lower priority,
            // if it counts to five minutes, we release snapshot if it exits
            if (rocksdb_it)
            {
                change_snapshot_for_iterator(last_refresh_rock_key);
                low_priority_cnt = 0;
            }
        }

        return;     
    }

    // We need refresh candidates

    // Refresh Step 1: free the memory of old candidates. 
    // NOTE: no need to lock so the memory deallocation not affect the performace of main thread
    reclaim_old_candidates();
    ++refresh_cnt;

    // Refresh step 2: deal with iterator
    if (rocksdb_it == NULL)
    {
        init_iterator();
    }
    else if (refresh_cnt == 61)
    {
        change_snapshot_for_iterator(last_refresh_rock_key);

        refresh_cnt = 1;
        low_priority_cnt = 0;       // we reset low_priority_cnt because snapshot is changed
    }

    // Refresh step 3: check whether the purge job finished
    if (!rocksdb_iter_valid(rocksdb_it))
    {
        check_rocksdb_iterator_error("do_purge() step 3");

        // purge job is finished
        serverLog(LL_NOTICE, "PURGEROCKSDB command finished in background!");
        
        // reclaim resource and init some state variable
        // reclaim_old_candidates();
        rocksdb_iter_destroy(rocksdb_it);
        rocksdb_it = NULL;
        refresh_cnt = 0;
        low_priority_cnt = 0;
        sdsfree(last_refresh_rock_key);
        last_refresh_rock_key = NULL;
        rock_p_lock();
        serverAssert(can_refresh_new_purge_candidates);     // gurarantee to be true, no need to reset
        server.rocksdb_purge_working = 0;       // open for next ROCKSDBPURGE command
        rock_p_unlock();

        return;
    }

    // Refresh step 4: refresh new candidates
    refresh_candidates(&last_refresh_rock_key);

    // wait for main thread to deal with these candidates
    rock_p_lock();
    can_refresh_new_purge_candidates = 0;           
    rock_p_unlock();
}

/*
 * The main entry for the thread of RocksDB purge
 */
static void* rock_purge_main(void* arg)
{
    UNUSED(arg);

    int loop = 0;
    while (loop == 0)
        atomicGet(rock_threads_loop_forever, loop);

    while (loop)
    {
        atomicGet(rock_threads_loop_forever, loop);

        if (loop)
        {
            refresh_rocksdb_stat();

            do_purge();

            usleep(1000*1000);      // purge thread wake every second
        }
    }

    return NULL;
}

/* Called in main thread for rock.c when main thread will exit */
void join_purge_thread()
{
    int s;
    void *res;

    s = pthread_join(rock_purge_thread_id, &res);
    if (s != 0)
    {
        serverLog(LL_WARNING, "rock purge thread join failure! err = %d", s);
    }
    else
    {
        serverLog(LL_NOTICE, "rock purge thread exit and join successfully.");
    }
}

void init_and_start_rock_purge_thread()
{
#ifdef RED_ROCK_MUTEX_DEBUG
    serverAssert(pthread_mutexattr_init(&mattr_purge) == 0);
    serverAssert(pthread_mutexattr_settype(&mattr_purge, PTHREAD_MUTEX_ERRORCHECK) == 0);
    serverAssert(pthread_mutex_init(&mutex_purge, &mattr_purge) == 0);
#endif

    rock_p_lock();
    server.rocksdb_purge_working = 0;
    can_refresh_new_purge_candidates = 1;
    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        purge_db_key_candidates[i] = NULL;
        purge_hash_key_candidates[i] = NULL;
        purge_hash_field_candidates[i] = NULL;
    }
    rock_p_unlock();

    rocksdb_it = NULL;

    if (pthread_create(&rock_purge_thread_id, NULL, rock_purge_main, NULL) != 0) 
        serverPanic("Unable to create a rock write thread.");
}

/* Called by main thread command interface
 */
void rock_purge_command(client *c)
{
    if (get_rocksdb_purge_working_state())
    {
        addReplyBulkCString(c, "RocksDB purge background job is working now...");
        return;
    }

    /* check memory usage */
    if (zmalloc_used_memory() >= get_max_rock_mem_of_os())
    {
        // memory needs to be evict, so it is not a good time for purge RocksDB
        addReplyBulkCString(c, "used memory is too high, the higher priorty background job is to evict to disk to free some memory which RedRock is just doing right now. Please try PURGEROCKSDB command later when the memory usage is ok.");
        return;
    }

    // set_rocksdb_purge_working_to_true();
    rock_p_lock();
    serverAssert(!server.rocksdb_purge_working);
    server.rocksdb_purge_working = 1;
    rock_p_unlock();

    addReplyBulkCString(c, "RocksDB purge job started in background!");
}

/* This is called in main thread cron
 */
void do_purge_in_cron()
{
    // check can_refresh_new_purge_candidates
    // If it is true, main thread needs do nothing because candidates are not ready
    rock_p_lock();
    int candidates_ready = !can_refresh_new_purge_candidates;
    rock_p_unlock();
    if (!candidates_ready)
        return;

    // main thread must guarantee that there are no evict job for write thread
    // i.e., by checking the eviction ring buffer is empty
    if (!is_eviction_ring_buffer_empty())
        return;

    static int purge_check_cnt = 0;
    if (has_unfinished_purge_task_for_write())
    {
        ++purge_check_cnt;
        if (purge_check_cnt >= 2)
        {
            purge_check_cnt = 0;
            try_to_wakeup_write_thread();
        }
        return;     // if write thread is busy for eviction or sleep, return
    }

    // transfer purge key to write thread
    // If no task (no purge key exists), then set can_refresh_new_purge_candidates to true
    int db_cnt = 0;
    int hash_cnt = 0;
    int db_dbids[ROCKSDB_PURGE_MAX_LEN];
    sds db_keys[ROCKSDB_PURGE_MAX_LEN];
    int hash_dbids[ROCKSDB_PURGE_MAX_LEN];
    sds hash_keys[ROCKSDB_PURGE_MAX_LEN];
    sds hash_fields[ROCKSDB_PURGE_MAX_LEN];

    rock_p_lock();

    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        if (purge_db_key_candidates[i] == NULL)
            break;

        int dbid = purge_db_dbids[i];
        sds key = purge_db_key_candidates[i];

        int need_delete = 0;

        redisDb *db = server.db + dbid;
        dictEntry *de = dictFind(db->dict, key);
        if (de == NULL)
        {
            // if not found in db
            need_delete = 1;
        }
        else
        {
            robj *o = dictGetVal(de);
            if (!is_rock_value(o))
                // found in db but not rock value
                need_delete = 1;
        }

        if (need_delete)
        {
            db_dbids[db_cnt] = dbid;
            db_keys[db_cnt] = key;
            ++db_cnt;
        }
    }

    for (int i = 0; i < ROCKSDB_PURGE_MAX_LEN; ++i)
    {
        if (purge_hash_key_candidates[i] == NULL)
            break;

        int dbid = purge_hash_dbids[i];
        sds hash_key = purge_hash_key_candidates[i];
        sds hash_field = purge_hash_field_candidates[i];
        
        int need_delete = 0;

        redisDb *db = server.db + dbid;
        dictEntry *de = dictFind(db->dict, hash_key);
        if (de == NULL)
        {
            // if not found in db
            need_delete = 1;
        }
        else
        {
            robj *o = dictGetVal(de);
            if (is_rock_value(o))
            {
                // if the whole key is rock value
                need_delete = 1;
            }
            else
            {
                if (!(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT))
                {
                    // if type and encoding not correct
                    need_delete = 1;
                }
                else
                {
                    dict *hash = o->ptr;
                    dictEntry *de_hash = dictFind(hash, hash_field);
                    if (de_hash == NULL)
                    {
                        // if field not exist
                        need_delete = 1;
                    }
                    else
                    {
                        sds field_val = dictGetVal(de_hash);
                        if (field_val != shared.hash_rock_val_for_field)
                            need_delete = 1;    // if field value is not rock field
                    }
                }
            }
        }

        if (need_delete)
        {
            hash_dbids[hash_cnt] = dbid;
            hash_keys[hash_cnt] = hash_key;
            hash_fields[hash_cnt] = hash_field;
            ++hash_cnt;
        }        
    }

    rock_p_unlock();

    if (db_cnt == 0 && hash_cnt == 0)
    {
        set_can_refressh_new_purge_candidates_to_true();
        return;
    }

    // transfer task to write thread
    transfer_purge_task_to_write_thread(db_cnt, db_dbids, db_keys, hash_cnt, hash_dbids, hash_keys, hash_fields);
}
