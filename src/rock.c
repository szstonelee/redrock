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
#include "rock_hash.h"
#include "rock_marshal.h"

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


static void debug_vc()
{
    client *vc = createClient(NULL);
    on_add_a_new_client(vc);

    const char *key_name = "abc";
    const char *field1_name = "f1";
    const char *field2_name = "f2";
    const char *val1_name = "val1";
    const char *val2_name = "val2";
    const int dbid = 0;
    redisDb *db = server.db + dbid;
    
    list *redis_keys = listCreate();
    sds need_key = sdsnew(key_name);
    // sds not_exist_kkey = sdsnew("not_exist_key");
    serverAssert(listAddNodeTail(redis_keys, need_key) != NULL);
    // serverAssert(listAddNodeTail(redis_keys, not_exist_kkey) != NULL);

    int cnt = 0;
    int sync_cnt = 0;
    while (1)
    {
        robj *key = createStringObject(key_name, strlen(key_name));
        robj *val1 = createStringObject(val1_name, strlen(val1_name));
        robj *val2 = createStringObject(val2_name, strlen(val2_name));
        sds field1 = sdsnew(field1_name);
        sds field2 = sdsnew(field2_name);

        robj *o = hashTypeLookupWriteOrCreate(vc, key);
        serverAssert(o != NULL);
        serverAssert(hashTypeSet(dbid, key->ptr, o, field1, val1->ptr, HASH_SET_COPY) == 0);
        serverAssert(hashTypeSet(dbid, key->ptr, o, field2, val2->ptr, HASH_SET_COPY) == 0);        

        while (1)
        {
            int ret = try_evict_one_key_to_rocksdb_by_rockevict_command(0, key->ptr);
            if (ret == TRY_EVICT_ONE_SUCCESS)
                break;
            serverAssert(ret == TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL);
        }

        usleep(1);
        list *left = check_ring_buf_first_and_recover_for_db(dbid, redis_keys);
        if (left && listLength(left) == 0)
            ++sync_cnt;
        if (left)
            listRelease(left);    
       
#if 0
        list *vals = get_vals_from_write_ring_buf_first_for_db(dbid, redis_keys);
        if (vals)
        {
            serverAssert(listLength(vals) == listLength(redis_keys));

            list *left = listCreate();
  
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
                    // need to recover data which is from ring buffer
                    // NOTE: no need to deal with rock_key_num in client. The caller will take care
                    dictEntry *de = dictFind(db->dict, redis_key);
                    // the redis_key must be here because it is from
                    // the caller on_client_need_rock_keys_for_db() which guaratee this
                    serverAssert(de);  

                    if (is_rock_value(dictGetVal(de)))      
                        // NOTE: the same key could repeat in redis_keys
                        //       so the second duplicated key, we can not guaratee it is rock value
                        dictGetVal(de) = unmarshal_object(recover_val);     // revocer in redis db
                }
            }
            
            listSetFreeMethod(vals, (void (*)(void*))sdsfree);
            listRelease(vals);

            listRelease(left);
        }
#endif

        serverAssert(dbDelete(db, key) == 1);

        decrRefCount(key);
        decrRefCount(val1);
        decrRefCount(val2);        
        sdsfree(field1);
        sdsfree(field2);       

        ++cnt;
        if (cnt == 10000)
        {
            serverLog(LL_WARNING, "used mem = %lu, sync_cnt = %d", zmalloc_used_memory(), sync_cnt);
            cnt = 0;
            sync_cnt = 0;
        } 
    }
}

/* For debug command, i.e. debugrock ... */
void debug_rock(client *c)
{
    sds flag = c->argv[1]->ptr;

    if (strcasecmp(flag, "mem") ==  0)
    {
        // debug_mem();
        debug_vc();
    }
    else if (strcasecmp(flag, "evictkeys") == 0 && c->argc >= 3)
    {
    }
    else if (strcasecmp(flag, "recoverkeys") == 0 && c->argc >= 3)
    {
    }
    else if (strcasecmp(flag, "testwrite") == 0) 
    {
    }
    else
    {
        addReplyError(c, "wrong flag for debugrock!");
        return;
    }

    addReplyBulk(c,c->argv[0]);
}

/* Encode the dbid with the input key for db.
 *
 * The first byte is the flag indicating the rock key is for db, 
 * i.e., only encode with db key (not hash key + hash field)
 * 
 * dbid will be encoded in one byte and be inserted in the second byte of the key, 
 * so dbid must greater than 0 and less than dbnum and 255.
 * 
 * NOTE: redis_to_rock_key's memory may be different after the calling which means 
 *       you need to use the return sds value for key in futrue.
 */
sds encode_rock_key_for_db(const int dbid, sds redis_to_rock_key)
{
    serverAssert(dbid >= 0 && dbid < server.dbnum && dbid <= 255);   

    size_t ken_len = sdslen(redis_to_rock_key); 
    redis_to_rock_key = sdsMakeRoomFor(redis_to_rock_key, 2);
    // memmove() is safe for overlapping and may be more efficient for word alignment
    memmove(redis_to_rock_key+2, redis_to_rock_key, ken_len);   
    redis_to_rock_key[0] = ROCK_KEY_FOR_DB;      
    redis_to_rock_key[1] = (unsigned char)dbid;
    sdsIncrLen(redis_to_rock_key, 2);

    return redis_to_rock_key;
}

/* Encode the dbid with the input key and field for the hash.
 *
 * The first byte is the flag indicating the rock key is for hash, 
 * i.e., encode with hash key and hash field.
 * 
 * dbid will be encoded in one byte and be inserted in the second byte of the key, 
 * so dbid must greater than 0 and less than dbnum and 255.
 * 
 * NOTE: hash_key_to_rock_key's memory may be different after the calling which means 
 *       you need to use the return sds value for key in futrue.
 */
sds encode_rock_key_for_hash(const int dbid, sds hash_key_to_rock_key, const sds hash_field)
{
    serverAssert(dbid >= 0 && dbid < server.dbnum && dbid <= 255);
    size_t key_len = sdslen(hash_key_to_rock_key);
    size_t field_len = sdslen(hash_field);
    hash_key_to_rock_key = sdsMakeRoomFor(hash_key_to_rock_key, 2 + sizeof(size_t) + field_len);
    memmove(hash_key_to_rock_key+2+sizeof(size_t), hash_key_to_rock_key, key_len);
    unsigned char* p = (unsigned char*)hash_key_to_rock_key;
    *p = ROCK_KEY_FOR_HASH;
    ++p;
    *p = (unsigned char)dbid;
    ++p;
    *((size_t*)p) = key_len;
    p += sizeof(size_t);
    p += key_len;
    memcpy(p, hash_field, field_len);
    sdsIncrLen(hash_key_to_rock_key, 2 + sizeof(size_t) + field_len);

    return hash_key_to_rock_key;
}

/* Decode the input rock_key.
 * dbid, redis_key, key_sz are the pointer to the result,
 * No memory allocation and the caller needs to guarantee the safety of rock_key
 * with the life time of redis_key and key_sz.
 */
void decode_rock_key_for_db(const sds rock_key, int* dbid, const char** redis_key, size_t* key_sz)
{
    serverAssert(sdslen(rock_key) >= 2);
    serverAssert(rock_key[0] == ROCK_KEY_FOR_DB);
    *dbid = rock_key[1];
    *redis_key = rock_key + 2;
    *key_sz = sdslen(rock_key) - 2;
}

/* Decode the input rock_key as a hash key.
 * dbid, key, key_sz, field, field_sz are the pointer to the result,
 * No memory allocation and the caller needs to guarantee the safety of rock_key.
 */
void decode_rock_key_for_hash(const sds rock_key, int *dbid, 
                              const char **key, size_t *key_sz,
                              const char **field, size_t *field_sz)
{
    serverAssert(sdslen(rock_key) >= 2 + sizeof(size_t));
    serverAssert(rock_key[0] == ROCK_KEY_FOR_HASH);
    *dbid = rock_key[1];
    size_t key_len = *((size_t*)(rock_key+2));
    serverAssert(sdslen(rock_key) >= 2 + sizeof(size_t) + key_len);
    *key = rock_key + 2 + sizeof(size_t);
    *key_sz = key_len;
    *field = rock_key + 2 + sizeof(size_t) + key_len;
    *field_sz = sdslen(rock_key) - 2 - sizeof(size_t) - key_len;
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
    shared.rock_cmd_fail = listCreate();
    listAddNodeHead(shared.rock_cmd_fail, NULL);    // NOTE: at lease one element in the list

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

    shared.rock_val_hash_ziplist = createHashObject();   // default is ziplist encoding
    makeObjectShared(shared.rock_val_hash_ziplist);

    shared.rock_val_hash_ht = createHashObject();
    hashTypeConvert(shared.rock_val_hash_ht, OBJ_ENCODING_HT);
    makeObjectShared(shared.rock_val_hash_ht);

    shared.rock_val_zset_ziplist = createZsetZiplistObject();
    makeObjectShared(shared.rock_val_zset_ziplist);

    shared.rock_val_zset_skiplist = createZsetObject();
    makeObjectShared(shared.rock_val_zset_skiplist);

    shared.hash_rock_val_for_field = NULL;      // NOTE: must be NULL for make sdsfree() do nothing
}

/* Called in main thread 
 * when a command is ready in buffer to process and need to check rock keys.
 * If the command does not need to check rock value (e.g., SET command)
 * return NULL.
 * If the command need to check and find no key in rock value
 * return NULL.
 * Otherwise, return a list (not empty) for those keys (sds) 
 * and the sds can point to the contents (argv) in client c.
 */
static list* get_keys_in_rock_for_command(const client *c, list **hash_keys, list **hash_fields)
{
    serverAssert(!is_client_in_waiting_rock_value_state(c));
    serverAssert(*hash_keys == NULL && *hash_fields == NULL);

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
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

    return cmd->rock_proc(c, hash_keys, hash_fields);
}


/* This is called in main thread by processCommand() before going into call().
 * Return value has three options:
 *
 * CHECK_ROCK_GO_ON_TO_CALL:  meaning the client is OK for call() for current command.
 * 
 * CHECK_ROCK_ASYNC_WAIT: indicating NOT going into call() because the client trap in rock state.
 * If the client trap into rock state, it will be in aysnc mode and recover from on_recover_data(),
 * which will later call processCommandAndResetClient() again in resume_command_for_client_in_async_mode()
 * in rock_read.c. processCommandAndResetClient() will call processCommand().
 * 
 * CHECK_ROCK_CMD_FAIL: the specific command check for argument failed and has replied to the client 
 *
 * If return 1, indicating NOT going into call() because the client trap in rock state.
 * Otherwise, return 0, meaning the client is OK for call() for current command.
 * 
 * NOTE: This function could be called by one client serveral times in aysnc mode
 *       for just processing ONE command 
 *       (e.g., mget <key1> <key2>, time 1: key1 is rock value but key2 is not, 
 *              after recover in async mode, <key2> became rock value)
 *       In the meantime, the key space could change, so the every check needs to
 *       consider this special situation.
 */
int check_and_set_rock_status_in_processCommand(client *c)
{
    serverAssert(!is_client_in_waiting_rock_value_state(c));

    // check and set rock state if there are some keys needed to read for async mode
    list *hash_keys = NULL;
    list *hash_fields = NULL;
    list *redis_keys = get_keys_in_rock_for_command(c, &hash_keys, &hash_fields);

    if (redis_keys == shared.rock_cmd_fail)
    {
        // The command specific checking (by copying the checking code from the specific command), 
        // is not passed and ther is an error reply for the user.
        // The caller does not need to call() (otherwise, there are double error reply)
        // and just go on for the socket buffer
        if (hash_keys) listRelease(hash_keys);
        if (hash_fields) listRelease(hash_fields);
        return CHECK_ROCK_CMD_FAIL;
    }

    // MUST deal with redis_keys first
    if (redis_keys)
    {
        serverAssert(listLength(redis_keys) > 0);
        on_client_need_rock_keys_for_db(c, redis_keys);
        listRelease(redis_keys);
    }

    // then deal with hash_keys and hash_fields
    if (hash_keys)
    {
        serverAssert(listLength(hash_keys) > 0);
        serverAssert(listLength(hash_keys) == listLength(hash_fields));
        on_client_need_rock_fields_for_hashes(c, hash_keys, hash_fields);
        listRelease(hash_keys);
        listRelease(hash_fields);
    }

    return is_client_in_waiting_rock_value_state(c) ? CHECK_ROCK_ASYNC_WAIT : CHECK_ROCK_GO_ON_TO_CALL;
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

/* Get one field for a hash from client's argv.
 * like HGET <key> <field>
 */
void generic_get_one_field_for_rock(const client *c, const sds key, const int index,
                                    list **hash_keys, list **hash_fields)
{
    serverAssert(index >= 1 && c->argc > index);

    dictEntry *de_db = dictFind(c->db->dict, key);
    if (de_db == NULL)
        return;

    robj *o = dictGetVal(de_db);
    if (!(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT))
        return;

    dict *hash = o->ptr;

    const sds field = c->argv[index]->ptr;
    dictEntry *de_hash = dictFind(hash, field);
    if (de_hash == NULL)
        return;

    const sds val = dictGetVal(de_hash);
    if (val != shared.hash_rock_val_for_field)
        return;

    list *join_keys = *hash_keys;
    list *join_fields = *hash_fields;

    if (join_keys == NULL)
    {
        serverAssert(join_fields == NULL);
        join_keys = listCreate();
        join_fields = listCreate();
    }
    serverAssert(listLength(join_keys) == listLength(join_fields));
    
    listAddNodeTail(join_keys, key);
    listAddNodeTail(join_fields, field);

    *hash_keys = join_keys;
    *hash_fields = join_fields;
}

/* For command like HMGET <key> <field1> <field2> ...
 * Reference generic_get_multi_keys_for_rock() for some help, it is similiar for index and step.
 */
void generic_get_multi_fields_for_rock(const client *c, const sds key, const int index, const int step,
                                       list **hash_keys, list **hash_fields)
{
    serverAssert(index >= 1 && c->argc > index);

    dictEntry *de_db = dictFind(c->db->dict, key);
    if (de_db == NULL)
        return;

    robj *o = dictGetVal(de_db);
    if (!(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT))
        return;

    dict *hash = o->ptr;

    list *join_keys = *hash_keys;
    list *join_fields = *hash_fields;

    for (int i = index; i < c->argc; i += step)
    {
        const sds field = c->argv[i]->ptr;

        dictEntry *de_hash = dictFind(hash, field);
        if (de_hash == NULL)
            continue;
        
        const sds val = dictGetVal(de_hash);
        if (val != shared.hash_rock_val_for_field)
            continue;

        if (join_keys == NULL)
        {
            serverAssert(join_fields == NULL);
            join_keys = listCreate();
            join_fields = listCreate();
        }
        serverAssert(listLength(join_keys) == listLength(join_fields));
        
        listAddNodeTail(join_keys, key);
        listAddNodeTail(join_fields, field);
    }

    *hash_keys = join_keys;
    *hash_fields = join_fields;
}

/* For command like HGETALL <key>
 */
void generic_get_all_fields_for_rock(const client *c, const sds key, list **hash_keys, list **hash_fields)
{
    dictEntry *de_db = dictFind(c->db->dict, key);
    if (de_db == NULL)
        return;

    robj *o = dictGetVal(de_db);
    if (!(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT))
        return;

    dict *hash = o->ptr;

    list *join_keys = *hash_keys;
    list *join_fields = *hash_fields;

    dictIterator *di = dictGetIterator(hash);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        const sds field = dictGetKey(de);
        const sds val = dictGetVal(de);

        if (val == shared.hash_rock_val_for_field)
        {
            if (join_keys == NULL)
            {
                serverAssert(join_fields == NULL);
                join_keys = listCreate();
                join_fields = listCreate();
            }
            serverAssert(listLength(join_keys) == listLength(join_fields));

            listAddNodeTail(join_keys, key);
            listAddNodeTail(join_fields, field);
        }
    }
    dictReleaseIterator(di);

    *hash_keys = join_keys;
    *hash_fields = join_fields;
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
void rock_evict(client *c)
{
    int keyIsExpired(redisDb *db, robj *key);       // in db.c

    if (c->flags & CLIENT_MULTI)
    {
        addReplyError(c, "ROCKEVICT can not in transaction!");
        return;
    }

    const char *already_rock_val = "ALREADY_ROCK_VAL_MAYBE_EXPIRE";
    const char *not_found = "NOT_FOUND";
    const char *expire_val = "EXPIRE_VALUE";
    const char *shared_val = "SHARED_VALUE_NO_NEED_TO_EVICT";
    const char *can_not_evict_type = "VALUE_TYPE_CAN_NOT_EVICT_LIKE_STREAM";
    const char *can_evict = "CAN_EVICT_AND_WRITTEN_TO_ROCKSDB";
    const char *alreay_in_rock_hash = "CAN_NOT_EVICT_BECAUSE_IT_IS_ROCK_HASH";

    serverAssert(c->argc > 1);
    const int key_num = c->argc - 1;
    addReplyArrayLen(c, key_num*2);
    
    redisDb *db = c->db;
    for (int i = 0; i < key_num; ++i)
    {
        const sds key = c->argv[i+1]->ptr;

        // Always reply key, then result
        addReplyBulk(c, c->argv[1]);

        robj *r = NULL;     // result

        if (keyIsExpired(db, c->argv[1]))
        {
            r = createStringObject(expire_val, strlen(expire_val));
            goto reply_result;
        }
           
        dictEntry *de = dictFind(db->dict, key);
        if (de == NULL)
        {
            r = createStringObject(not_found, strlen(not_found));
            goto reply_result;            
        }

        const robj *val = dictGetVal(de);
        if (is_rock_value(val))
        {
            r = createStringObject(already_rock_val, strlen(already_rock_val));
            goto reply_result;
        }

        if(is_shared_value(val))
        {
            r = createStringObject(shared_val, strlen(shared_val));
            goto reply_result;
        }

        if (!is_evict_value(val))
        {
            r = createStringObject(can_not_evict_type, strlen(can_not_evict_type));
            goto reply_result;
        }

        if (is_in_rock_hash(db->id, key))
        {
            r = createStringObject(alreay_in_rock_hash, strlen(alreay_in_rock_hash));
            goto reply_result;
        }
 
        // can try to evcit this key
        while (r == NULL)
        {
            const int ret = try_evict_one_key_to_rocksdb_by_rockevict_command(db->id, key);
                        
            switch (ret)
            {
            case TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL:
                // NOTE: if RocksDB is busy, it may increase latency because 
                // here is main thread
                // For cron eviction we use time out
                break;      // loop continue
                        
            case TRY_EVICT_ONE_SUCCESS:
                r = createStringObject(can_evict, strlen(can_evict));
                break;

            default:
                serverPanic("try_evict_one_key_to_rocksdb() for rock_evict() return unknow!");
                break;
            }
        }

reply_result:
        serverAssert(r != NULL);
        addReplyBulk(c, r);
        decrRefCount(r);
    }
}

/* rockevict <key> <field> <field> ...
 */
void rock_evict_hash(client *c)
{
    int keyIsExpired(redisDb *db, robj *key);       // in db.c

    if (c->flags & CLIENT_MULTI)
    {
        addReplyError(c, "ROCKEVICTHASH can not in transaction!");
        return;
    }

    redisDb *db = c->db;
    if (keyIsExpired(db, c->argv[1]))
    {
        addReplyError(c, "ROCKEVICTHASH can not evict an expired key");
        return;
    }
    
    if (keyIsExpired(db, c->argv[1]))
    {
        addReplyError(c, "key is expried");
        return; 
    }

    const int dbid = db->id;
    const sds key = c->argv[1]->ptr;
    dictEntry *de = dictFind(db->dict, key);
    if (de == NULL)
    {
        addReplyError(c, "can not find the key");
        return;
    }

    if (is_rock_value(dictGetVal(de)))
    {
        addReplyError(c, "the whole key is already rock value");
    }

    robj *o = dictGetVal(de);
    if (!(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT))
    {
        addReplyError(c, "not hash key or hash key not encoded as ht");
        return;
    }

    if (is_rock_value(o))
    {
        addReplyError(c, "hask key's value alreay in RocksDB totally");
        return;
    }

    if (!is_in_rock_hash(dbid, key))
    {
        addReplyError(c, "hask key not in rock hash, try add more fields");
        return;
    }

    const char *not_found = "field not found";
    const char *already_evict = "field already evicted";
    const char *can_evict = "field successfully evicted";

    serverAssert(c->argc > 2);
    dict *hash = o->ptr;
    const int field_num = c->argc - 2;
    addReplyArrayLen(c, field_num*2);

    for (int i = 0; i < field_num; ++i)
    {
        // alwyas reply field, then result
        const sds field = c->argv[i+2]->ptr;
        addReplyBulk(c, c->argv[i+2]);

        robj *r = NULL;     // result

        dictEntry *de = dictFind(hash, field);
        if (de == NULL)
        {
            r = createStringObject(not_found, strlen(not_found));
            goto reply_result;
        }

        sds val = dictGetVal(de);
        if (val == shared.hash_rock_val_for_field)
        {
            r = createStringObject(already_evict, strlen(already_evict));
            goto reply_result;
        }

        // can try to evcit this field
        while (r == NULL)
        {
            const int ret = try_evict_one_field_to_rocksdb_by_rockevithash_command(dbid, key, field);
            switch (ret)
            {
            case TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL:
                // NOTE: if RocksDB is busy, it may increase latency because 
                // here is main thread
                // For cron eviction we use time out
                break;      // loop continue

            case TRY_EVICT_ONE_SUCCESS:
                r = createStringObject(can_evict, strlen(can_evict));
                break;

            default:
                serverPanic("try_evict_one_key_to_rocksdb() for rock_evict_hash() return unknow!");
            }
        }

reply_result:
        serverAssert(r != NULL);
        addReplyBulk(c, r);
        decrRefCount(r);
    }    
}