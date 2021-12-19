// macros for nftw()
#define _XOPEN_SOURCE 700
#ifndef USE_FDS
#define USE_FDS 15
#endif
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

#include "rock.h"
#include "server.h"
#include "rock_write.h"
#include "rock_read.h"

#include <dirent.h>
#include <ftw.h>

redisAtomic int rock_threads_loop_forever;

/* Global rocksdb handler for rock_read.c and rock_write.c */
rocksdb_t* rockdb = NULL;

static int unlink_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    UNUSED(sb);
    UNUSED(typeflag);
    UNUSED(ftwbuf);

    int rv = remove(fpath);

    if (rv)
        perror(fpath);

    return rv;
}

static void rek_mkdir(char *path) 
{
    char *sep = strrchr(path, '/');
    if (sep != NULL) 
    {
        *sep = 0;
        rek_mkdir(path);
        *sep = '/';
    }

    if (sep != NULL && mkdir(path, 0777) && errno != EEXIST)
        serverLog(LL_WARNING, "error while trying to create folder = %s", path); 
}

/* Init the global rocksdb handler, i.e., rockdb. */
#define ROCKSDB_LEVEL_NUM   7
void init_rocksdb(const char* folder_original_path)
{
    // We add listening port to folder_path
    sds folder_path = sdsnewlen(folder_original_path, strlen(folder_original_path));
    sds listen_port = sdsfromlonglong(server.port);
    serverLog(LL_NOTICE, "init rocksdb, server listen port = %s", listen_port);
    folder_path = sdscatsds(folder_path, listen_port);
    sdsfree(listen_port);
    folder_path = sdscat(folder_path, "/");

    atomicSet(rock_threads_loop_forever, 1);

    // verify last char, must be '/'
    const size_t path_len = strlen(folder_path);
    if (folder_path[path_len-1] != '/')
    {
        serverLog(LL_WARNING, "RocksDB folder path must be ended of slash char of /");
        exit(1);
    }

    // nftw(folder_path, unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
    DIR *dir = opendir(folder_path);
    if (dir)
    {
        closedir(dir);
        if (nftw(folder_path, unlink_cb, 10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS) < 0)
        {
            serverLog(LL_WARNING, "remove RocksDB folder failed, folder = %s", folder_path);
            perror("ERROR: ntfw");
            exit(1);
        }
        serverLog(LL_NOTICE, "finish removal of the whole RocksDB folder = %s", folder_path);
    }
    // check again
    DIR *check_dir = opendir(folder_path);
    if (check_dir)
    {
        closedir(check_dir);
        serverLog(LL_WARNING, "rocksdb folder still exists = %s", folder_path);
        exit(1);
    } 
    else if (ENOENT != errno)
    {
        serverLog(LL_WARNING, "opendir(%s) failed for errono = %d", folder_path, errno);
        exit(1);
    }
    // mkdir 
    mode_t mode = 0777;
    if (mkdir(folder_path, mode)) 
    {
        if (errno == ENOENT) 
        {
            // folder not exist
            sds copy_folder = sdsnew(folder_path);
            rek_mkdir(copy_folder);
            sdsfree(copy_folder);
            return;
        } 
        else 
        {
            serverPanic("Can not mkdir %s with mode 777, errno = %d",  folder_path, errno);
        }
    }

    rocksdb_options_t *options = rocksdb_options_create();

    // Set # of online cores
    const long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    rocksdb_options_increase_parallelism(options, (int)(cpus));
    rocksdb_options_optimize_level_style_compaction(options, 0); 
    // create the DB if it's not already present
    rocksdb_options_set_create_if_missing(options, 1);
    // file size
    rocksdb_options_set_target_file_size_base(options, 4<<20);
    // memtable
    rocksdb_options_set_write_buffer_size(options, 32<<20);     // 32M memtable size
    rocksdb_options_set_max_write_buffer_number(options, 2);    // memtable number
    // WAL
    // rocksdb_options_set_manual_wal_flush(options, 1);    // current RocksDB API 6.20.3 not support
    // compaction (using Universal Compaction)
    rocksdb_options_set_compaction_style(options, rocksdb_universal_compaction);
    rocksdb_options_set_num_levels(options, ROCKSDB_LEVEL_NUM);   
    rocksdb_options_set_level0_file_num_compaction_trigger(options, 4);
    // set each level compression types (reference RocksDB API of compression_type.h)
    int compression_level_types[ROCKSDB_LEVEL_NUM];
    for (int i = 0; i < ROCKSDB_LEVEL_NUM; ++i) 
    {
        if (i == 0 || i == 1) 
        {
            compression_level_types[i] = 0x0;   // kNoCompression
        } 
        else 
        {
            compression_level_types[i] = 0x04;      // kLZ4Compression
        }
    }
    rocksdb_options_set_compression_per_level(options, compression_level_types, ROCKSDB_LEVEL_NUM);
    // table options
    rocksdb_options_set_max_open_files(options, 1024);      // if default is -1, no limit, and too many open files consume memory
    rocksdb_options_set_table_cache_numshardbits(options, 4);        // shards for table cache
    rocksdb_block_based_table_options_t *table_options = rocksdb_block_based_options_create();
    // block size (Although the RocksDB website recommend 16K-32K in production), we need a test for 4K or 8K in debug
#if DEBUG
    rocksdb_block_based_options_set_block_size(table_options, 4<<10);
#else
    rocksdb_block_based_options_set_block_size(table_options, 16<<10);
#endif
    // block cache
    rocksdb_cache_t *lru_cache = rocksdb_cache_create_lru(256<<20);        // 256M lru cache
    rocksdb_block_based_options_set_block_cache(table_options, lru_cache);
    // index in cache and partitioned index filter (https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters)
    rocksdb_block_based_options_set_index_type(table_options, rocksdb_block_based_table_index_type_two_level_index_search);
    rocksdb_block_based_options_set_partition_filters(table_options, 1);
    rocksdb_block_based_options_set_metadata_block_size(table_options, 4<<10);
    // filter and index in block cache to save memory
    rocksdb_block_based_options_set_cache_index_and_filter_blocks(table_options, 1);    
    rocksdb_block_based_options_set_pin_top_level_index_and_filter(table_options, 1);
    rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(table_options, 1);
    // NOTE: we use universal compaction, so not set pin_l0_filter_and_index_blocks_in_cache
    // rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(table_options, 1);

    // bloom filter
    rocksdb_filterpolicy_t *bloom = rocksdb_filterpolicy_create_bloom_full(10);
    rocksdb_block_based_options_set_filter_policy(table_options, bloom);
    // need invest, maybe mix with rocksdb_options_optimize_level_style_compaction()
    // rocksdb_options_set_max_background_jobs(options, 3);     

    rocksdb_options_set_block_based_table_factory(options, table_options);

    // open DB
    char *err = NULL;
    rockdb = rocksdb_open(options, folder_path, &err);
    if (err) 
        serverPanic("initRocksdb() failed reason = %s", err);

    sdsfree(folder_path);
}



/*
static sds debug_random_sds(const int max_len)
{
    int rand_len = random() % max_len;

    sds s = sdsempty();
    s = sdsMakeRoomFor(s, max_len);
    for (int i = 0; i < rand_len; ++i)
    {
        const char c = 'a' + (random() % 26);
        s = sdscatlen(s, &c, 1);
    }
    return s;
}
*/

/* For debug command, i.e. debugrock ... */
void debug_rock(client *c)
{
    sds flag = c->argv[1]->ptr;

    if (strcasecmp(flag, "evictkeys") == 0 && c->argc >= 3)
    {
        sds keys[RING_BUFFER_LEN];
        int dbids[RING_BUFFER_LEN];
        int len = 0;
        for (int i = 0; i < c->argc-2; ++i)
        {
            sds input_key = c->argv[i+2]->ptr;
            dictEntry *de = dictFind(c->db->dict, input_key);
            if (de)
            {
                if (!is_rock_value(dictGetVal(de)))
                {
                    serverLog(LL_NOTICE, "debug evictkeys, try key = %s", input_key);
                    keys[len] = input_key;
                    dbids[len] = c->db->id;
                    ++len;
                }
            }
        }
        if (len)
        {
            int ecvict_num = try_evict_to_rocksdb(len, dbids, keys);
            serverLog(LL_NOTICE, "debug evictkeys, ecvict_num = %d", ecvict_num);
        }
    }
    else if (strcasecmp(flag, "recoverkeys") == 0 && c->argc >= 3)
    {
        serverAssert(c->rock_key_num == 0);
        list *rock_keys = listCreate();
        for (int i = 0; i < c->argc-2; ++i)
        {
            sds input_key = c->argv[i+2]->ptr;
            dictEntry *de = dictFind(c->db->dict, input_key);
            if (de)
            {
                if (is_rock_value(dictGetVal(de)))
                {
                    serverLog(LL_WARNING, "debug_rock() found rock key = %s", (sds)dictGetKey(de));
                    listAddNodeTail(rock_keys, dictGetKey(de));
                }
            }
        }
        if (listLength(rock_keys) != 0)
        {
            on_client_need_rock_keys(c, rock_keys);
        }
        listRelease(rock_keys);
    }
    else if (strcasecmp(flag, "testread") == 0)
    {
        /*
        sds keys[2];
        keys[0] = sdsnew("key1");
        keys[1] = sdsnew("key2");
        sds copy_keys[2];
        copy_keys[0] = sdsdup(keys[0]);
        copy_keys[1] = sdsdup(keys[1]);
        robj* objs[2];
        char* val1 = "val_for_key1";
        char* val2 = "val_for_key2";       
        objs[0] = createStringObject(val1, strlen(val1));
        objs[1] = createStringObject(val2, strlen(val2));
        int dbids[2];
        dbids[0] = 65;      // like letter 'a'
        dbids[1] = 66;      // like letter 'b'
        write_batch_append_and_abandon(2, dbids, keys, objs);
        sleep(1);       // waiting for save to RocksdB
        debug_add_tasks(2, dbids, copy_keys);
        on_delete_key(dbids[0], copy_keys[1]);
        */
    }
    else if (strcasecmp(flag, "testwrite") == 0) 
    {
        /*
        int dbid = 1;

        int val_len = random() % 1024;
        sds val = sdsempty();
        for (int i = 0; i < val_len; ++i)
        {
            val = sdscat(val, "v");
        }

        sds keys[RING_BUFFER_LEN];
        robj* objs[RING_BUFFER_LEN];
        int dbids[RING_BUFFER_LEN];
        int cnt = 0;
        while (cnt < 5000000)
        {
            int space = space_in_write_ring_buffer();
            if (space == 0)
            {
                serverLog(LL_NOTICE, "space = 0, sleep for a while, cnt = %d", cnt);
                usleep(10000);
                continue;
            }
            
            int random_pick = random() % RING_BUFFER_LEN;
            if (random_pick == 0)
                random_pick = 1;
            
            const int pick = random_pick < space ? random_pick : space;

            for (int i = 0; i < pick; ++i)
            {
                sds key = debug_random_sds(128);
                sds val = debug_random_sds(1024);

                keys[i] = key;
                robj* o = createStringObject(val, sdslen(val));
                sdsfree(val);
                objs[i] = o;
                dbids[i] = dbid;
            }         
            write_batch_append_and_abandon(pick, dbids, keys, objs); 
            cnt += pick;
            serverLog(LL_NOTICE, "write_batch_append total = %d, cnt = %d", space, cnt);
        }
        */
    }
    else
    {
        addReplyError(c, "wrong flag for debugrock!");
        return;
    }

    addReplyBulk(c,c->argv[0]);
}

/* Encode the dbid with the input key, dbid will be encoded in one byte 
 * and be inserted in the head of the key, 
 * so dbid must greater than 0 and less than dbnum and 127.
 * NOTE: For 127, it is because char is signed or unsigned. (gcc use unsigned as default but can change) 
 * We try to not use more memory for one byte insertion.
 * NOTE: key's memory may be different after the calling which means 
 *       you need to use the return sds value for key in futrue.
 */
sds encode_rock_key(const int dbid, sds redis_to_rock_key)
{
    serverAssert(dbid >= 0 && dbid < server.dbnum && dbid <= 127);
    // memmove() is safe for overlapping and may be more efficient for word alignment
    redis_to_rock_key = sdsMakeRoomFor(redis_to_rock_key, 1);
    memmove(redis_to_rock_key+1, redis_to_rock_key, sdslen(redis_to_rock_key));         
    redis_to_rock_key[0] = (char)dbid;
    sdsIncrLen(redis_to_rock_key, 1);

    return redis_to_rock_key;
}

/* Decode the input rock_key.
 * dbid, key, sz are the pointer to the result 
 * so caller needs the address the instances of them.
 * No memory allocation and the caller needs to prevent the safety of rock_key.
 */
void decode_rock_key(const sds rock_key, int* dbid, char** redis_key, size_t* key_sz)
{
    *dbid = rock_key[0];
    *redis_key = rock_key+1;
    serverAssert(sdslen(rock_key) >= 1);
    *key_sz = sdslen(rock_key) - 1;
}

/* for client id to client* hash table and rock.c readCandidatesDictType */
static inline uint64_t dictUint64Hash(const void *key) {
    return (uint64_t)key;
}

/* client id hash table 
 * key: client id, uint64_t type, always unique for current Redis server
 * value: client* pointer */
dictType clientIdDictType = {
    dictUint64Hash,             /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    NULL,                       /* key compare */
    NULL,                       /* key destructor. NULL because we store the clientid in the key pointer */
    NULL,                       /* val destructor. NULL because we store the client pointer */
    NULL                        /* allow to expand */
};

static dict* client_id_table = NULL;

void init_client_id_table()
{
    client_id_table = dictCreate(&clientIdDictType, NULL);
}

/* If not exists, return NULL.
 */
client* lookup_client_from_id(const uint64_t client_id)
{
    dictEntry *de = dictFind(client_id_table, (const void*)client_id);
    return de == NULL ? NULL : dictGetVal(de);
}

void on_add_a_new_client(client* const c)
{
    int res = dictAdd(client_id_table, (void*)c->id, (void*)c);
    serverAssert(res == DICT_OK);
    c->rock_key_num = 0;
}

void on_del_a_destroy_client(const client* const c)
{
    uint64_t key = c->id;
    client* check = lookup_client_from_id(key);
    serverAssert(check == c);
    int res = dictDelete(client_id_table, (void*)key);
    serverAssert(res == DICT_OK);
}

/* Create all shared objects for rock. 
 * For each Redis type, like string, ziplist, hash, we need to create the same type of shared.
 * And we need to make them not be deleted.
 */
void create_shared_object_for_rock()
{
    const char *str = "shared str";
    shared.rock_val_str_other = createStringObject(str, strlen(str));
    makeObjectShared(shared.rock_val_str_other);

    long long val = 123456;
    shared.rock_val_str_int = createStringObjectFromLongLong(val);
    makeObjectShared(shared.rock_val_str_int);
    
    shared.rock_val_list_quicklist = createQuicklistObject();
    makeObjectShared(shared.rock_val_list_quicklist);

    shared.rock_val_set_int = createIntsetObject();
    makeObjectShared(shared.rock_val_set_int);

    shared.rock_val_set_ht = createSetObject();
    makeObjectShared(shared.rock_val_set_ht);

    shared.rock_val_hash_ht = createObject(OBJ_HASH, NULL);
    shared.rock_val_hash_ht->encoding = OBJ_ENCODING_HT;
    makeObjectShared(shared.rock_val_hash_ht);

    shared.rock_val_hash_ziplist = createObject(OBJ_HASH, NULL);
    shared.rock_val_hash_ziplist->encoding = OBJ_ENCODING_ZIPLIST;
    makeObjectShared(shared.rock_val_hash_ziplist);

    shared.rock_val_zset_ziplist = createObject(OBJ_ZSET, NULL);
    shared.rock_val_zset_ziplist->encoding = OBJ_ENCODING_ZIPLIST;
    makeObjectShared(shared.rock_val_zset_ziplist);

    shared.rock_val_zset_skiplist = createZsetObject();
    makeObjectShared(shared.rock_val_zset_skiplist);
}

/* Called in main thread 
 * when a command is ready in buffer to process and need to check rock keys.
 * If the command does not need to check rock value (e.g., SET command)
 * return NULL.
 * If the command need to check and find no key in rock value
 * return NULL.
 * Otherwise, return a list (not empty) for those keys (sds) 
 * and the sds can use the contents in client c.
 */
static list* get_keys_in_rock_for_command(const client *c)
{
    serverAssert(c->rock_key_num == 0);

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
    /*
    if (cmd == NULL)
        return NULL;
    */
    serverAssert(cmd);

    if (c->flags & CLIENT_MULTI)
    {
        // if client is in transaction mode
        if (cmd->proc != execCommand)
            // if query commands in trannsaction mode or DISCARD/WATCH/UNWATCH
            return NULL;        
    }

    // the command does not need to check rock key, e.g., set <key> <val>
    if (cmd->rock_proc == NULL) 
        return NULL;

    list *redis_keys = cmd->rock_proc(c);
    if (redis_keys == NULL)
    {
        return NULL;        // no rock value found
    }
    else
    {
        serverAssert(listLength(redis_keys) != 0);
        return redis_keys;
    }
}


/* This is called in main thread by processCommand() before going into call().
 * If return 1, indicating NOT going into call() because the client trap in rock state.
 * Otherwise, return 0, meaning the client is OK for call() for current command.
 * If the client trap into rock state, it will be in aysnc mode and recover from on_recover_data(),
 * which will later call processCommandAndResetClient() again in resume_command_for_client_in_async_mode()
 * in rock_read.c. processCommandAndResetClient() will call processCommand().
 */
int check_and_set_rock_status_in_processCommand(client *c)
{
    serverAssert(!is_client_in_waiting_rock_value_state(c));

    // check and set rock state if there are some keys needed to read for async mode
    list *rock_keys = get_keys_in_rock_for_command(c);
    if (rock_keys)
    {
        on_client_need_rock_keys(c, rock_keys);
        listRelease(rock_keys);
    }

    return is_client_in_waiting_rock_value_state(c);
}

/* Get one key from client's argv. 
 * Usually index is 1. e.g., GET <key>
 * index is the index in argv of client 
 * */
list* generic_get_one_key_for_rock(const client *c, const int index)
{
    serverAssert(index >= 1 && c->argc > index);

    redisDb *db = c->db;
    const sds key = c->argv[index]->ptr;

    dictEntry *de = dictFind(db->dict, key);

    if (de == NULL)
        return NULL;

    robj *o = dictGetVal(de);
    if (!is_rock_value(o))
        return NULL;

    list *keys = listCreate();
    listAddNodeTail(keys, key);
    return keys;
}

/* Get multi keys from client's argv from range [start, end).
 * E.g., BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
 * start is the start index in argv, usually 1.
 * end is the ennd index (NOTE: not include) in argv.
 */
list* generic_get_multi_keys_for_rock_in_range(const client *c, const int start, const int end)
{
    serverAssert(start >= 1 && start < end && end <= c->argc);

    redisDb *db = c->db;

    list *keys = NULL;
    for (int i = start; i < end; ++i)
    {
        const sds key = c->argv[i]->ptr;

        dictEntry *de = dictFind(db->dict, key);
        if (de == NULL)
            continue;
        
        robj *o = dictGetVal(de);
        if (!is_rock_value(o))
            continue;

        if (keys == NULL)
            keys = listCreate();

        listAddNodeTail(keys, key);
    }
    return keys;
}

/* Get multi keys from client's argv exclude the last tail_cnt (NOTE: could be zero)
 * For tail_cnt == 0, E.g.,  MGET <key1> <key2> ...
 * For tail_cnt != 0, E.g., BRPOP key [key ...] timeout
 * 
 * index is the start index for argv, usually 1.
 * the end is till the end of argv.
 * step is the jumping space for the search, usually 1. 
 * */
list* generic_get_multi_keys_for_rock_exclude_tails(const client *c, const int index, 
                                                    const int step, const int tail_cnt)
{
    serverAssert(index >= 1 && c->argc > index && step >= 1 && tail_cnt >= 0);
    serverAssert(c->argc - tail_cnt >= index + 1);

    redisDb *db = c->db;
    
    list *keys = NULL;
    for (int i = index; i < c->argc - tail_cnt; i += step)
    {
        const sds key = c->argv[i]->ptr;

        dictEntry *de = dictFind(db->dict, key);
        if (de == NULL)
            continue;
        
        robj *o = dictGetVal(de);
        if (!is_rock_value(o))
            continue;

        if (keys == NULL)
            keys = listCreate();

        listAddNodeTail(keys, key);
    }
    return keys;
}

/* Get multi kkeys from client's argv. E.g., MGET <key1> <key2> ...
 * index is the start index for argv, usually 1.
 * the end is till the end of argv.
 * step is the jumping space for the search, usually 1. 
 * */
list* generic_get_multi_keys_for_rock(const client *c, const int index, const int step)
{
    return generic_get_multi_keys_for_rock_exclude_tails(c, index, step, 0);
}

/* Some zset commands, like ZINTERSTORE, ZDIFF ZINTRE, 
 * IF have_dest != 0, the argv[1] is desination key, argv[2] is the number of key.
 * If have_dest == 0, the argv[1] is the number of key
 * keys follow the number.
 */
list* generic_get_zset_num_for_rock(const client *c, const int have_dest)
{
    redisDb *db = c->db;
    list *keys = NULL;
    long long num = 0;
    if (have_dest != 0)
    {
        serverAssert(c->argc >= 4);
        robj *o_num = c->argv[2];
        const int ret = getLongLongFromObject(o_num, &num);
        serverAssert(ret == C_OK);

        robj *o_dest = c->argv[1];
        const sds dest = o_dest->ptr;
        dictEntry *de = dictFind(db->dict, dest);
        if (de)
        {
            robj *val = dictGetVal(de);
            if (is_rock_value(val))
            {
                keys = listCreate();
                listAddNodeTail(keys, dest);
            }
        }
    }
    else
    {
        serverAssert(c->argc >= 3);
        robj *o = c->argv[1];
        const int ret = getLongLongFromObject(o, &num);
        serverAssert(ret == C_OK);
    }
    serverAssert(num > 0);

    const int start = have_dest != 0 ? 3 : 2;
    for (int i = 0; i < num; ++i)
    {
        const sds key = c->argv[start+i]->ptr;

        dictEntry *de = dictFind(db->dict, key);
        if (de == NULL)
            continue;

        robj *o = dictGetVal(de);
        if (!is_rock_value(o))
            continue;

        if (keys == NULL)
            keys = listCreate();

        listAddNodeTail(keys, key);
    }

    return keys;
}

/* Main thread waiting for read thread and write thread exit */
void wait_rock_threads_exit()
{
    // signal rock threads to exit
    atomicSet(rock_threads_loop_forever, 0);

    int s;
    void *res;

    s = pthread_join(rock_write_thread_id, &res);
    if (s != 0)
    {
        serverLog(LL_WARNING, "rock write thread join failure!");
    }
    else
    {
        serverLog(LL_NOTICE, "rock write thread exit and join successfully.");
    }
    
    s = pthread_join(rock_read_thread_id, &res);
    if (s != 0)
    {
        serverLog(LL_WARNING, "rock read thread join failure");
    }
    else
    {
        serverLog(LL_NOTICE, "rock read thread exit and join successfully.");
    }

    if (rockdb)
        rocksdb_close(rockdb);
}

/* rock_evict <key> ...
 */ 
int keyIsExpired(redisDb *db, robj *key);       // in db.c
void rock_evict(client *c)
{
    const int key_num = c->argc - 1;
    addReplyArrayLen(c, key_num*2);

    redisDb *db = c->db;
    sds already_rock_val = sdsnew("ALREADY_ROCK_VAL_MAYBE_EXPIRE");
    sds not_found = sdsnew("NOT_FOUND");
    sds expire_val = sdsnew("EXPIRE_VALUE");
    sds shared_val = sdsnew("SHARED_VALUE_NO_NEED_TO_EVICT");
    sds can_not_evict_type = sdsnew("VALUE_TYPE_CAN_NOT_EVICT_LIKE_STREAM");
    sds can_evict = sdsnew("CAN_EVICT_AND_WRITTEN_TO_ROCKSDB");

    for (int i = 0; i < key_num; ++i)
    {
        const sds key = c->argv[i+1]->ptr;

        robj *o_key = createStringObject(key, sdslen(key));
        addReplyBulk(c, o_key);
    
        robj *r = NULL;
        dictEntry *de = dictFind(db->dict, key);
        if (de == NULL)
        {
            r = createStringObject(not_found, sdslen(not_found));
        }
        else
        {
            robj *val = dictGetVal(de);
            if (is_rock_value(val))
            {
                r = createStringObject(already_rock_val, sdslen(already_rock_val));
            }
            else if(is_shared_value(val))
            {
                r = createStringObject(shared_val, sdslen(shared_val));
            }
            else if (!is_evict_value(val))
            {
                r = createStringObject(can_not_evict_type, sdslen(can_not_evict_type));
            }
            else
            {
                if (keyIsExpired(db, o_key))
                {
                    r = createStringObject(expire_val, sdslen(expire_val));
                }
                else
                {
                    // can evcit this key
                    while (!try_evict_one_key_to_rocksdb(db->id, key));     // loop until success

                    r = createStringObject(can_evict, sdslen(can_evict));
                }
            }
        }

        addReplyBulk(c, r);

        decrRefCount(o_key);
        decrRefCount(r);
    }

    sdsfree(already_rock_val);
    sdsfree(not_found);
    sdsfree(expire_val);
    sdsfree(can_evict);
    sdsfree(shared_val);
    sdsfree(can_not_evict_type);
}