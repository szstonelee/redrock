// macros for nftw()
#ifdef __APPLE__
// nothing
#else       // linux
#define _XOPEN_SOURCE 700
#ifndef USE_FDS
#define USE_FDS 15
#endif
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64
#endif

#include "rock.h"
// #include "server.h"
#include "rock_write.h"
#include "rock_read.h"
#include "rock_hash.h"
#include "rock_marshal.h"
#include "rock_evict.h"

#include <dirent.h>
#include <ftw.h>
#include <unistd.h>

/* Control when read thread and write thread should exit loop */ 
redisAtomic int rock_threads_loop_forever;

/* Global rocksdb handler for rock_read.c and rock_write.c */
rocksdb_t* rockdb = NULL;

/* For Rock visit total */
static long long stat_key_total;
static long long stat_key_rock;
static long long stat_field_total;
static long long stat_field_rock;

void get_visit_stat_for_rock(size_t *key_total_visits, size_t *key_rock_visits,
                             size_t *field_total_visits, size_t *field_rock_visits)
{
    *key_total_visits = stat_key_total > 0 ? (size_t)stat_key_total : 0;
    *key_rock_visits = stat_key_rock > 0 ? (size_t)stat_key_rock : 0;
    *field_total_visits = stat_field_total > 0 ? (size_t)stat_field_total : 0;
    *field_rock_visits = stat_field_rock > 0 ? (size_t)stat_field_rock : 0;
}

void init_stat_rock_key_and_field()
{
    stat_key_total = 0;
    stat_key_rock = 0;
    stat_field_total = 0;
    stat_field_rock = 0;
}

/*
void get_stat_rock_key_and_field(long long *key_total, long long *key_rock,
                                 long long *field_total, long long *field_rock)
{
    *key_total = stat_key_total;
    *key_rock = stat_key_rock;
    *field_total = stat_field_total;
    *field_rock = stat_field_rock;
}
*/

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


/* For debug command, i.e. debugrock ... Only enable when you want debug RedRock */
#if 0
void debug_rock(client *c)
{
    sds flag = c->argv[1]->ptr;

    if (strcasecmp(flag, "evictkeyreport") == 0) 
    {
        #ifdef RED_ROCK_EVICT_INFO
        eviction_info_print();
        #endif
    }
    else if (strcasecmp(flag, "evictfield") == 0)
    {
        // perform_field_eviction(10);
    }
    else if (strcasecmp(flag, "evictkey") == 0)
    {
        // perform_key_eviction(70);
    }
    else if (strcasecmp(flag, "mem") ==  0)
    {
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
#endif

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

    const long long val = 123456;   // NOTE: can not too small which will be shared object
    shared.rock_val_str_int = createStringObjectFromLongLong(val);
    serverAssert(shared.rock_val_str_int->encoding == OBJ_ENCODING_INT);
    makeObjectShared(shared.rock_val_str_int);

    const char *str = "shared str";
    shared.rock_val_str_other = createStringObject(str, strlen(str));
    makeObjectShared(shared.rock_val_str_other);
    
    shared.rock_val_list_quicklist = createQuicklistObject();
    makeObjectShared(shared.rock_val_list_quicklist);

    shared.rock_val_set_int = createIntsetObject();
    makeObjectShared(shared.rock_val_set_int);

    shared.rock_val_set_ht = createSetObject();
    makeObjectShared(shared.rock_val_set_ht);

    shared.rock_val_hash_ziplist = createHashObject();   // default is ziplist encoding
    makeObjectShared(shared.rock_val_hash_ziplist);

    shared.rock_val_hash_ht = createHashObject();
    hashTypeConvert(shared.rock_val_hash_ht, OBJ_ENCODING_HT);  // NOTE: We must conver to an empty dict
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
        // is not passed and there is an error reply for the user.
        // The caller does not need to call() (otherwise, there are double error reply)
        // and just go on for the socket buffer
        if (hash_keys) listRelease(hash_keys);
        if (hash_fields) listRelease(hash_fields);
        return CHECK_ROCK_CMD_FAIL;
    }

    // MUST deal with redis_keys first
    // because c.rock_key_num =
    if (redis_keys)
    {
        serverAssert(listLength(redis_keys) > 0);
        on_client_need_rock_keys_for_db(c, redis_keys);
        listRelease(redis_keys);
    }

    // then deal with hash_keys and hash_fields
    // becausse c.rock_key_num +=
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

/* For script and module. Before the call(), we need check the command's rock value 
 * and recover them in sync mode (as soon as possible). So the call() can not fail for rock value.
 *
 * We use sync mode because the call() in script is part of atomic operation (like MULTI ... EXEC)
 * 
 * return 1 means should go on for call(). otherwise(0), means skip call()
 */
int check_and_recover_rock_value_in_sync_mode(client *c)
{
    // NOTE: we need disable c multi state if have,  e.g. more than one redis scommand in scripts
    //       for get_keys_in_rock_for_command()
    const int have_multi_state = c->flags & CLIENT_MULTI;
    if (have_multi_state)
        c->flags &= ~CLIENT_MULTI;

    list *hash_keys = NULL;
    list *hash_fields = NULL;
    list *redis_keys = get_keys_in_rock_for_command(c, &hash_keys, &hash_fields);

    if (redis_keys == shared.rock_cmd_fail)
    {
        // check the above
        if (hash_keys) listRelease(hash_keys);
        if (hash_fields) listRelease(hash_fields);
        if (have_multi_state)
            c->flags |= CLIENT_MULTI;       // recover multi state if have
        return 0;
    }

    // NOTE: unlike the above, if (redis_keys) and if (hash_keys) can has any order

    if (redis_keys)
    {
        on_client_need_rock_keys_for_db_in_sync_mode(c, redis_keys);
        listRelease(redis_keys);
    }

    if (hash_keys)
    {
        on_client_need_rock_fields_for_hash_in_sync_mode(c, hash_keys, hash_fields);
        listRelease(hash_keys);
        listRelease(hash_fields);
    }

    if (have_multi_state)
        c->flags |= CLIENT_MULTI;       // recover
    return 1;
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

    ++stat_key_total;

    robj *o = dictGetVal(de);
    if (!is_rock_value(o))
        return NULL;

    list *keys = listCreate();
    listAddNodeTail(keys, key);
    ++stat_key_rock;
    return keys;
}

/* Get one field for a hash from client's argv.
 * like HGET <key> <field>
 */
void generic_get_one_field_for_rock(const client *c, const sds key, const int index,
                                    list **hash_keys, list **hash_fields)
{
    serverAssert(index >= 2 && c->argc > index);

    dictEntry *de_db = dictFind(c->db->dict, key);
    if (de_db == NULL)
        return;

    const robj *o = dictGetVal(de_db);
    if (!(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT))
        return;

    /* We treat it as field stat, not key stat */
    serverAssert(stat_key_total > 0);
    --stat_key_total;
    ++stat_field_total;

    serverAssert(!is_rock_value(o));

    const dict *hash = o->ptr;

    const sds field = c->argv[index]->ptr;
    dictEntry *de_hash = dictFind((dict*)hash, field);
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
    ++stat_field_rock;

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

    /* We treat it as file stat not key stat */
    serverAssert(stat_key_total > 0);
    --stat_key_total;

    dict *hash = o->ptr;

    list *join_keys = *hash_keys;
    list *join_fields = *hash_fields;

    for (int i = index; i < c->argc; i += step)
    {
        ++stat_field_total;

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
        ++stat_field_rock;
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

    /* We treat it as field stat, not key stat */
    serverAssert(stat_key_total > 0);
    --stat_key_total;

    dict *hash = o->ptr;

    list *join_keys = *hash_keys;
    list *join_fields = *hash_fields;

    dictIterator *di = dictGetIterator(hash);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        const sds field = dictGetKey(de);
        const sds val = dictGetVal(de);

        ++stat_field_total;

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
            ++stat_field_rock;
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

        ++stat_key_total;
        
        robj *o = dictGetVal(de);
        if (!is_rock_value(o))
            continue;

        if (keys == NULL)
            keys = listCreate();

        listAddNodeTail(keys, key);
        ++stat_key_rock;
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
        
        ++stat_key_total;

        robj *o = dictGetVal(de);
        if (!is_rock_value(o))
            continue;

        if (keys == NULL)
            keys = listCreate();

        listAddNodeTail(keys, key);
        ++stat_key_rock;
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
            ++stat_key_total;
            robj *val = dictGetVal(de);
            if (is_rock_value(val))
            {
                keys = listCreate();
                listAddNodeTail(keys, dest);
                ++stat_key_rock;
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

        ++stat_key_total;

        robj *o = dictGetVal(de);
        if (!is_rock_value(o))
            continue;

        if (keys == NULL)
            keys = listCreate();

        listAddNodeTail(keys, key);
        ++stat_key_rock;
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

/* Check whether a redis_key can be evicted to RocksDB as a whole key.
 *
 * If the following conditions match, we can not evict the key.
 * 1. not exist in redis db
 * 2. has expired
 * 3. already evicted
 * 4. is shared value
 * 5. is not supported type as stream and module
 * 6. is in rock hash which means we will evict the fields not the keys 
 * 7. already in candidates, because for aysnc mode, the candidates key could
 *    recover for any key has rock value. So if in candidates, we can not 
 *    evict the key otherwise we get the wrong value. 
 *    The candidates will vanish after the async work finished,
 *    so we can try the key later.
 * 
 * Otherwise, return CHECK_EVICT_OK to indicate that the key is valid for eviction.
 */
int check_valid_evict_of_key_for_db(const int dbid, const sds redis_key)
{
    redisDb *db = server.db + dbid;

    int keyIsExpired(redisDb *db, robj *key);       // declaration in db.c
    robj *o_key = createStringObject(redis_key, sdslen(redis_key));
    if (keyIsExpired(db, o_key))
    {
        expireIfNeeded(db, o_key);  // try to expire the key (only master node can delete)
        decrRefCount(o_key);
        return CHECK_EVICT_EXPIRED;
    }
    decrRefCount(o_key);

    dictEntry *de_db = dictFind(db->dict, redis_key);
    if (de_db == NULL)
        return CHECK_EVICT_NOT_FOUND;    

    const robj *val = dictGetVal(de_db);

    if (is_rock_value(val))
        return CHECK_EVICT_ALREADY_WHOLE_ROCK_VALUE;
    
    if (is_shared_value(val))
        return CHECK_EVICT_SHARED_VALUE;

    if (is_not_supported_evict_type(val))
        return CHECK_EVICT_NOT_SUPPORTED_TYPE;

    if (already_in_candidates_for_db(dbid, redis_key))
        return CHECK_EVICT_IN_CANDIDAES;

    /* The following is special for db key */
    if (is_in_rock_hash(dbid, redis_key))
        // if a hash key already in rock hash, it can not be 
        // evicted to RocksDB as a whole key
        // because we will evict the fields for the key
        return CHECK_EVICT_ALREADY_IN_ROCK_HASH_FOR_DB_KEY;

    return CHECK_EVICT_OK;
}

/* Check whether a redis_key can be evicted to RocksDB as a whole key.
 *
 * Check the above check_valid_evict_of_key_for_db() for the can-not-evict conditions.
 * NOTE: for 6, it is different, the hash_key and hash_field must be in rock hash.
 *       and one more check for the key is hash type with encoding hash.
 * 
 * Otherwise, return CHECK_EVICT_OK to indicate that the key is valid for eviction.
 */
int check_valid_evict_of_key_for_hash(const int dbid, const sds hash_key, const sds hash_field)
{
    redisDb *db = server.db + dbid;

    int keyIsExpired(redisDb *db, robj *key);       // declaration in db.c
    robj *o_key = createStringObject(hash_key, sdslen(hash_key));
    if (keyIsExpired(db, o_key))
    {
        expireIfNeeded(db, o_key);  // try to expire the key (only master node can delete)
        decrRefCount(o_key);
        return CHECK_EVICT_EXPIRED;
    }
    decrRefCount(o_key);

    dictEntry *de_db = dictFind(db->dict, hash_key);
    if (de_db == NULL)
        return CHECK_EVICT_NOT_FOUND;

    const robj *val = dictGetVal(de_db);

    if (is_rock_value(val))
        return CHECK_EVICT_ALREADY_WHOLE_ROCK_VALUE;

    if (is_shared_value(val))
        return CHECK_EVICT_SHARED_VALUE;

    if (is_not_supported_evict_type(val))
        return CHECK_EVICT_NOT_SUPPORTED_TYPE;

    if (already_in_candidates_for_hash(dbid, hash_key, hash_field))
        return CHECK_EVICT_IN_CANDIDAES;

    /* The following is special for rock hash */
    robj *o = dictGetVal(de_db);
    if (!(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT))
        return CHECK_EVICT_TYPE_OR_ENCODING_WRONG_FOR_FIELD;

    dictEntry *de_hash = dictFind(o->ptr, hash_field);
    if (de_hash == NULL)
        return CHECK_EVICT_NOT_FOUND_FIELD;
    
    if (!is_in_rock_hash(dbid, hash_key))
        return CHECK_EVICT_NOT_IN_ROCK_HASH_FOR_FIELD;

    sds field_val = dictGetVal(de_hash);
    if (field_val == shared.hash_rock_val_for_field)
        return CHECK_EVICT_ALREAY_FIELD_ROCK_VALUE;

    return CHECK_EVICT_OK;
}

/* rock_evict <key> ...
 */ 
void rock_evict(client *c)
{
    if (c->flags & CLIENT_MULTI)
    {
        addReplyError(c, "ROCKEVICT can not in transaction!");
        return;
    }

    const char *can_evict = "CAN_EVICT_AND_WRITTEN_TO_ROCKSDB";

    const char *expire_val = "EXPIRE_VALUE";
    const char *not_found = "NOT_FOUND_KEY";
    const char *already_rock_val = "ALREADY_WHOLE_ROCK_VAL";
    const char *shared_val = "SHARED_VALUE_NO_NEED_TO_EVICT";
    const char *not_supported = "CAN_NOT_EVICT_FOR_NOT_SUPPORTED_TYPE";
    const char *already_in_candidates = "CAN_NOT_EVICT_FOR_IN_CANDIDATES_TRY_LATER";

    // special for db key
    const char *alreay_in_rock_hash = "CAN_NOT_EVICT_BECAUSE_IT_IS_ROCK_HASH";

    serverAssert(c->argc > 1);
    const int key_num = c->argc - 1;
    addReplyArrayLen(c, key_num*2);
    
    const int dbid = c->db->id;
    for (int i = 0; i < key_num; ++i)
    {
        const sds key = c->argv[i+1]->ptr;

        // Always reply key, then result
        addReplyBulk(c, c->argv[1]);

        const char *r = NULL;     // result

        const int check_evict = check_valid_evict_of_key_for_db(dbid, key);
        switch (check_evict)
        {
        case CHECK_EVICT_OK:
            break;

        case CHECK_EVICT_EXPIRED:
            r = expire_val;
            break;

        case CHECK_EVICT_NOT_FOUND:
            r = not_found;
            break;

        case CHECK_EVICT_ALREADY_WHOLE_ROCK_VALUE:
            r = already_rock_val;
            break;

        case CHECK_EVICT_SHARED_VALUE:
            r = shared_val;
            break;

        case CHECK_EVICT_NOT_SUPPORTED_TYPE:
            r = not_supported;
            break;

        case CHECK_EVICT_IN_CANDIDAES:
            r = already_in_candidates;
            break;

        case CHECK_EVICT_ALREADY_IN_ROCK_HASH_FOR_DB_KEY:
            r = alreay_in_rock_hash;
            break;

        default:
            serverPanic("rock_evict() failed for check_valid_evict_of_key_for_db() = %d", check_evict);
        }
 
        // can try to evcit this key
        while (r == NULL)
        {
            const int ret = try_evict_one_key_to_rocksdb(dbid, key, NULL);
                        
            switch (ret)
            {
            case TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL:
                // NOTE: if RocksDB is busy, it may increase latency because 
                // here is main thread
                // For cron eviction we use time out
                break;      // loop continue
                        
            case TRY_EVICT_ONE_SUCCESS:
                r = can_evict;
                break;

            default:
                serverPanic("try_evict_one_key_to_rocksdb() for rock_evict() return unknow!");
            }
        }

        serverAssert(r != NULL);
        addReplyBulkCString(c, r);
    }
}

/* rockevict <key> <field> <field> ...
 */
void rock_evict_hash(client *c)
{
    if (c->flags & CLIENT_MULTI)
    {
        addReplyError(c, "ROCKEVICTHASH can not in transaction!");
        return;
    }

    const char *can_evict = "CAN_EVICT_AND_WRITTEN_TO_ROCKSDB";

    const char *expire_val = "EXPIRE_VALUE";
    const char *not_found = "NOT_FOUND_KEY";
    const char *already_whole_rock_val = "ALREADY_WHOLE_ROCK_VAL";
    const char *shared_val = "SHARED_VALUE_NO_NEED_TO_EVICT";
    const char *not_supported = "CAN_NOT_EVICT_FOR_NOT_SUPPORTED_TYPE";
    const char *already_in_candidates = "CAN_NOT_EVICT_FOR_IN_CANDIDATES_TRY_LATER";

    // special for rock hash
    const char *wrong_type_or_encoding = "CAN_NOT_EVICT_FOR_WRONG_TYPE_OR_ENCODING";
    const char *not_found_field = "NOT_FOUND_FIELD";
    const char *not_in_rock_hash = "CAN_NOT_EVICT_BECAUSE_NOT_IN_ROCK_HASH";
    const char *already_field_rock_val = "ALREADY_FIELD_ROCK_VAL";

    serverAssert(c->argc > 2);
    const int field_num = c->argc - 2;
    addReplyArrayLen(c, field_num*2);

    const int dbid = c->db->id;
    const sds hash_key = c->argv[1]->ptr;
    for (int i = 0; i < field_num; ++i)
    {
        const sds hash_field = c->argv[i+2]->ptr;
        addReplyBulk(c, c->argv[i+2]);  // alwyas reply field, then result

        const char *r = NULL;     // result

        const int check_evict = check_valid_evict_of_key_for_hash(dbid, hash_key, hash_field);
        switch (check_evict)
        {
        case CHECK_EVICT_OK:
            break;

        case CHECK_EVICT_EXPIRED:
            r = expire_val;
            break;

        case CHECK_EVICT_NOT_FOUND:
            r = not_found;
            break;

        case CHECK_EVICT_ALREADY_WHOLE_ROCK_VALUE:
            r = already_whole_rock_val;
            break;

        case CHECK_EVICT_SHARED_VALUE:
            r = shared_val;
            break;

        case CHECK_EVICT_NOT_SUPPORTED_TYPE:
            r = not_supported;
            break;

        case CHECK_EVICT_IN_CANDIDAES:
            r = already_in_candidates;
            break;

        case CHECK_EVICT_TYPE_OR_ENCODING_WRONG_FOR_FIELD:
            r = wrong_type_or_encoding;
            break;

        case CHECK_EVICT_NOT_IN_ROCK_HASH_FOR_FIELD:
            r = not_in_rock_hash;
            break;

        case CHECK_EVICT_NOT_FOUND_FIELD:
            r = not_found_field;
            break;

        case CHECK_EVICT_ALREAY_FIELD_ROCK_VALUE:
            r = already_field_rock_val;
            break;

        default:
            serverPanic("rock_evict() failed for check_valid_evict_of_key_for_db() = %d", check_evict);
        }

        // can try to evcit this field
        while (r == NULL)
        {
            const int ret = 
                try_evict_one_field_to_rocksdb(dbid, hash_key, hash_field, NULL);
            switch (ret)
            {
            case TRY_EVICT_ONE_FAIL_FOR_RING_BUFFER_FULL:
                // NOTE: if RocksDB is busy, it may increase latency because 
                // here is main thread
                // For cron eviction we use time out
                break;      // loop continue

            case TRY_EVICT_ONE_SUCCESS:
                r = can_evict;
                break;

            default:
                serverPanic("try_evict_one_key_to_rocksdb() for rock_evict_hash() return unknow!");
            }
        }

        serverAssert(r != NULL);
        addReplyBulkCString(c, r);
    }    
}

void get_rock_info(int *no_zero_dbnum,
                   size_t *total_key_num, 
                   size_t *total_rock_evict_num, 
                   size_t *total_key_in_disk_num,
                   size_t *total_rock_hash_num,
                   size_t *total_rock_hash_field_num,
                   size_t *total_field_in_disk_num)
{
    for (int i = 0; i < server.dbnum; ++i)
    {
        int this_db_zero = 1;

        redisDb *db = server.db + i;
        *total_key_num += dictSize(db->dict);

        const size_t key_cnt = dictSize(db->rock_evict);
        *total_rock_evict_num += key_cnt;
        if (key_cnt != 0)
            this_db_zero = 0;

        *total_key_in_disk_num += db->rock_key_in_disk_cnt;
        *total_field_in_disk_num += db->rock_field_in_disk_cnt;

        const size_t hash_cnt = dictSize(db->rock_hash);
        *total_rock_hash_num += hash_cnt;
        if (hash_cnt != 0)
            this_db_zero = 0;

        *total_rock_hash_field_num += db->rock_hash_field_cnt;

        if (!this_db_zero)
            ++(*no_zero_dbnum);
    }
}

/* For Linux, it is OK to get avail mem at runtiime, but for MacOS，it can't.
 * So for MacOS, we return SIZE_MAX which means it is impossible
 */
size_t get_free_mem_of_os()
{
#if defined(__APPLE__)
    return SIZE_MAX;
#else   // linux
    return sysconf(_SC_AVPHYS_PAGES) * sysconf(_SC_PAGESIZE);
#endif    
}

static void exit_for_not_enough_free_mem_when_startup(const size_t need_mem, const size_t current_mem)
{
    void bytesToHuman(char *s, unsigned long long n);   // declaration in server.c
    
    char need_hmem[64];
    char current_hmem[64];
    bytesToHuman(need_hmem, need_mem);
    bytesToHuman(current_hmem, current_mem);
    serverLog(LL_WARNING, "free mem is too low, for this machine at least %s, current is %s. "
                          "You can 'sync; echo 1 > /proc/sys/vm/drop_caches' "
                          "to flush page cache of OS to make more free memory and try again!", 
                          need_hmem, current_hmem);
    exit(1);
}

void check_mem_requirement_on_startup()
{
    const size_t sys_mem = server.system_memory_size;
    if (sys_mem < (4ULL<<30))
    {
        serverLog(LL_WARNING, "Your system memory is too low (at least 4G), " 
                              "system memory for this machine only = %zu",
                              sys_mem); 
        exit(1);
    }

    const size_t free_mem = get_free_mem_of_os();       // MacOS is SIZE_MAX

    size_t need_free_mem = 0;

    if (sys_mem <= (5ULL<<30))
    {
        need_free_mem = 2ULL<<30;
        if (free_mem < need_free_mem)
            exit_for_not_enough_free_mem_when_startup(need_free_mem, free_mem);
    }
    else if (sys_mem <= (7ULL<<30))
    {
        need_free_mem = 3ULL<<30;
        if (free_mem < need_free_mem)
            exit_for_not_enough_free_mem_when_startup(need_free_mem, free_mem);        
    }
    else if (sys_mem <= (15ULL<<30))
    {
        need_free_mem = 4ULL<<30;
        if (free_mem < need_free_mem)
            exit_for_not_enough_free_mem_when_startup(need_free_mem, free_mem);
    }
    else if (sys_mem <= (31ULL<<30))
    {
        need_free_mem = 5ULL<<30;
        if (free_mem < need_free_mem)
            exit_for_not_enough_free_mem_when_startup(need_free_mem, free_mem);
    }
    else
    {
        need_free_mem = 6ULL<<30;
        if (free_mem < need_free_mem)
            exit_for_not_enough_free_mem_when_startup(need_free_mem, free_mem);
    }
}


/* return the config least free memory in bytes.
 * 
 * 1) if server.leastfreemem == 0, 
 * it means the value is defined by system, 
 * return different size for differnt machine.
 * 
 * For system defined, the least free memory do not use too much,
 * because the page os (buff/cache in free -h) is not accounted for. 

 * 2) if negative, it means disable least free mem checking.
 * 
 * 3) otherwise, it is up to the user. Any thing possible for the user responsibility. 
 */
static long long get_least_free_mem_in_bytes()
{
    unsigned long long least_free_mem = server.leastfreemem;
    if (least_free_mem == 0)
    {
        // system defined least free memory
        if (server.system_memory_size >= (10ULL<<30))
        {
            // if this machine has memory more than 10G, we set least free mem to 256M
            least_free_mem = 256ULL<<20;
        }
        else
        {
            // otherwise, 128M
            least_free_mem = 128ULL<<20;
        }
    }
    return least_free_mem;
}

unsigned long long get_max_rock_mem_of_os()
{
    if (server.maxrockmem != 0)
        return server.maxrockmem;       // used-defined

    const size_t init_free_mem = server.initial_free_mem;

    if (init_free_mem == SIZE_MAX)
    {
        // For MacOS
        const size_t sys_mem = server.system_memory_size;
        if (sys_mem >= (10ULL<<30))
        {
            return sys_mem - (4ULL<<30);
        }
        else if (sys_mem >= (5ULL<<30))
        {
            return sys_mem - (3500ULL<<20);
        }
        else
        {
            serverAssert(sys_mem > (2ULL<<30));
            return sys_mem - (2ULL<<30);
        }
    }
    else
    {
        // Linux
        serverAssert(init_free_mem > (2ULL<<30));
        return init_free_mem - (2ULL<<30);
    }
}

/* For command rockstat */
void rock_stat(client *c)
{
    void bytesToHuman(char *s, unsigned long long n);   // declaration in server.c

    addReplyArrayLen(c, 4);

    sds s; 

    // line 1 : memory
    s = sdsempty();
    char hmem[64];
    const size_t redis_used_mem = zmalloc_used_memory();
    bytesToHuman(hmem, redis_used_mem);
    char total_system_hmem[64];
    bytesToHuman(total_system_hmem, server.system_memory_size);
    char peak_hmem[64];
    bytesToHuman(peak_hmem, server.stat_peak_memory);
    char free_hmem[64];
    bytesToHuman(free_hmem, get_free_mem_of_os());
    char least_free_hmem[64];
    const long long least_free_mem = get_least_free_mem_in_bytes();
    if (least_free_mem < 0)
    {
        strncpy(least_free_hmem, "leastfreemem_disabled", 64);
    }
    else
    {
        bytesToHuman(least_free_hmem, (size_t)least_free_mem);
    }
    char max_rock_hmem[64];
    bytesToHuman(max_rock_hmem, get_max_rock_mem_of_os());
    char used_memory_rss_hmem[64];
    const size_t rss_mem = server.cron_malloc_stats.process_rss;
    bytesToHuman(used_memory_rss_hmem, rss_mem);
    const size_t rocksdb_mem = rss_mem > redis_used_mem ? rss_mem - redis_used_mem : 0;
    char rocksdb_hmem[64];
    bytesToHuman(rocksdb_hmem, rocksdb_mem);
    s = sdscatprintf(s, 
                    "used_human = %s, used_peak_human = %s, sys_human = %s, "
                    "free_hmem = %s, least_free_hmem = %s, max_rock_hmem = %s, "
                    "rss_hmem = %s, rocksdb(and other) = %s", 
                    hmem, peak_hmem, total_system_hmem, 
                    free_hmem, least_free_hmem, max_rock_hmem,
                    used_memory_rss_hmem, rocksdb_hmem);
    addReplyBulkCString(c, s);
    sdsfree(s);

    // line 2 : rock info
    s = sdsempty();
    int no_zero_dbnum = 0;
    size_t total_key_num = 0;
    size_t total_rock_evict_num = 0;
    size_t total_key_in_disk_num = 0;
    size_t total_rock_hash_num = 0;
    size_t total_rock_hash_field_num = 0;
    size_t total_field_in_disk_num = 0;    
    get_rock_info(&no_zero_dbnum, &total_key_num, 
                  &total_rock_evict_num, &total_key_in_disk_num, 
                  &total_rock_hash_num, &total_rock_hash_field_num, &total_field_in_disk_num);

    s = sdscatprintf(s, 
                     "no_zero_dbnum = %d, key_num = %zu, evict_key_num = %zu, key_in_disk_num = %zu, "
                     "evict_hash_num = %zu, evict_field_num = %zu, field_in_disk_num = %zu",
                     no_zero_dbnum, total_key_num, total_rock_evict_num, total_key_in_disk_num, 
                     total_rock_hash_num, total_rock_hash_field_num, total_field_in_disk_num);    
    addReplyBulkCString(c, s);
    sdsfree(s);

    // line 3: config
    s = sdsempty();
    s = sdscatprintf(s, "hash-max-ziplist-entries = %zu, hash-max-rock-entries = %zu",
                     server.hash_max_ziplist_entries, server.hash_max_rock_entries);
    addReplyBulkCString(c, s);
    sdsfree(s);

    // line 4: rock stat for key and field
    s = sdsempty();
    s = sdscatprintf(s, 
                     "stat_key_total = %lld, stat_key_rock = %lld, key_percent = %d(%%), "
                     "stat_field_total = %lld, stat_field_rock = %lld, field_percent = %d(%%)",
                     stat_key_total, stat_key_rock, stat_key_total == 0 ? 0 : (int)(100*stat_key_rock/stat_key_total),
                     stat_field_total, stat_field_rock, stat_field_total == 0 ? 0 : (int)(100*stat_field_rock/stat_field_total));
    addReplyBulkCString(c, s);
    sdsfree(s);
}

#define EVICT_OUTPUT_PERCENTTAGE 100

static int cal_rock_all_for_evict()
{
    int total = 0;

    int all_empty = 1;

    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;
        size_t num = dictSize(db->rock_evict);
        
        if (num == 0)
            continue;

        all_empty = 0;

        total += 1;     // this db info if size is not zero
        
        if (num < EVICT_OUTPUT_PERCENTTAGE)
        {
            total += 1;     // one line output for small db
        }
        else
        {
            total += EVICT_OUTPUT_PERCENTTAGE;       // percentage output
        }
    }

    if (all_empty)
        total = 1;      // all empty, one line output

    return total;
}

static int cal_rock_all_for_hash()
{
    int total = 0;

    int all_empty = 1;

    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;
        size_t num = db->rock_hash_field_cnt;
        
        if (num == 0)
            continue;

        all_empty = 0;

        total += 1;     // this db info if size is not zero
        
        if (num < EVICT_OUTPUT_PERCENTTAGE)
        {
            total += 1;     // one line output for small db
        }
        else
        {
            total += EVICT_OUTPUT_PERCENTTAGE;       // percentage output
        }
    }

    if (all_empty)
        total = 1;      // all empty, one line output

    return total;
}

static void rock_all_for_evict_for_db(const int dbid)
{
    redisDb *db = server.db + dbid;
    serverAssert(dictSize(db->rock_evict) != 0 && dictSize(db->rock_evict) < EVICT_OUTPUT_PERCENTTAGE);

    list *l = listCreate();

    dictIterator *di = dictGetIterator(db->rock_evict);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        const sds key = dictGetKey(de);
        listAddNodeTail(l, key);
    }
    dictReleaseIterator(di);

    listIter li;
    listNode *ln;
    listRewind(l, &li);
    while ((ln = listNext(&li)))
    {
        const sds key = listNodeValue(ln);
        while (try_evict_one_key_to_rocksdb(dbid, key, NULL) != TRY_EVICT_ONE_SUCCESS);
    }

    listRelease(l);
}


static void rock_all_for_hash_for_db(const int dbid)
{
    redisDb *db = server.db + dbid;
    serverAssert(db->rock_hash_field_cnt != 0 && db->rock_hash_field_cnt < EVICT_OUTPUT_PERCENTTAGE);

    list *l_key = listCreate();
    list *l_field = listCreate();

    dictIterator *di = dictGetIterator(db->rock_hash);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        const sds key = dictGetKey(de);
        dict *lrus = dictGetVal(de);

        dictIterator *di_lrus = dictGetIterator(lrus);
        dictEntry *de_lrus;
        while ((de_lrus = dictNext(di_lrus)))
        {
            const sds field = dictGetKey(de_lrus);
            listAddNodeTail(l_key, key);
            listAddNodeTail(l_field, field);
        }
        dictReleaseIterator(di_lrus);        
    }
    dictReleaseIterator(di);

    listIter li_key;
    listNode *ln_key;
    listRewind(l_key, &li_key);

    listIter li_field;
    listNode *ln_field;
    listRewind(l_field, &li_field);

    while ((ln_key = listNext(&li_key)))
    {
        ln_field = listNext(&li_field);

        const sds key = listNodeValue(ln_key);
        const sds field = listNodeValue(ln_field);
        while (try_evict_one_field_to_rocksdb(dbid, key, field, NULL) != TRY_EVICT_ONE_SUCCESS);
    }

    listRelease(l_key);
    listRelease(l_field);
}

static void rock_all_for_evict_for_db_by_one_percentage(client *c, const int dbid,
                                                        const int p_index, const size_t scope,
                                                        listIter *li, listNode **ln)
{
    monotime timer;
    elapsedStart(&timer);

    for (size_t i = 0; i < scope; ++i)
    {
        *ln = listNext(&(*li));
        const sds key = listNodeValue(*ln);
        while (try_evict_one_key_to_rocksdb(dbid, key, NULL) != TRY_EVICT_ONE_SUCCESS);
    }

    sds s = sdsempty();
    if (EVICT_OUTPUT_PERCENTTAGE != 100)
    {
        s = sdscatprintf(s, "key evict %d (1/%d), scope = %zu, time = %llu (ms)", 
                            p_index, EVICT_OUTPUT_PERCENTTAGE, scope, (long long unsigned)elapsedMs(timer));
    }
    else
    {
        s = sdscatprintf(s, "key evict %d%%, scope = %zu, time = %llu (ms)", 
                            p_index, scope, (long long unsigned)elapsedMs(timer));
    }

    if (!server.cluster_enabled)
        addReplyBulkCString(c, s);

    sdsfree(s);
}

static void rock_all_for_hash_for_db_by_one_percentage(client *c, const int dbid,
                                                       const int p_index, const size_t scope,
                                                       listIter *li_key, listNode **ln_key,
                                                       listIter *li_field, listNode **ln_field)
{
    monotime timer;
    elapsedStart(&timer);

    for (size_t i = 0; i < scope; ++i)
    {
        *ln_key = listNext(&(*li_key));
        *ln_field = listNext(&(*li_field));

        const sds key = listNodeValue(*ln_key);
        const sds field = listNodeValue(*ln_field);

        while (try_evict_one_field_to_rocksdb(dbid, key, field, NULL) != TRY_EVICT_ONE_SUCCESS);
    }

    sds s = sdsempty();
    if (EVICT_OUTPUT_PERCENTTAGE != 100)
    {
        s = sdscatprintf(s, "field evict %d (1/%d), scope = %zu, time = %llu (ms)", 
                            p_index, EVICT_OUTPUT_PERCENTTAGE, scope, (long long unsigned)elapsedMs(timer));
    }
    else
    {
        s = sdscatprintf(s, "field evict %d%%, scope = %zu, time = %llu (ms)", 
                            p_index, scope, (long long unsigned)elapsedMs(timer));
    }

    if (!server.cluster_enabled)
        addReplyBulkCString(c, s);

    sdsfree(s);
}

static void rock_all_for_evict_for_db_by_percentage(client *c, const int dbid)
{
    redisDb *db = server.db + dbid;
    serverAssert(dictSize(db->rock_evict) >= EVICT_OUTPUT_PERCENTTAGE);

    list *l = listCreate();

    dictIterator *di = dictGetIterator(db->rock_evict);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        const sds key = dictGetKey(de);
        listAddNodeTail(l, key);
    }
    dictReleaseIterator(di);

    listIter li;
    listNode *ln;
    listRewind(l, &li);

    const size_t total = dictSize(db->rock_evict);
    const size_t scope = total / EVICT_OUTPUT_PERCENTTAGE;
    serverAssert(scope > 0);
    for (int i = 1; i <= EVICT_OUTPUT_PERCENTTAGE; ++i)
    {
        if (i != EVICT_OUTPUT_PERCENTTAGE)
        {
            rock_all_for_evict_for_db_by_one_percentage(c, dbid, i, scope, &li, &ln);
        }
        else
        {
            const size_t last_scope = total - scope * (EVICT_OUTPUT_PERCENTTAGE-1);
            serverAssert(last_scope > 0);
            rock_all_for_evict_for_db_by_one_percentage(c, dbid, i, last_scope, &li, &ln);            
        }
    }

    listRelease(l);
}

static void rock_all_for_hash_for_db_by_percentage(client *c, const int dbid)
{
    redisDb *db = server.db + dbid;
    serverAssert(db->rock_hash_field_cnt >= EVICT_OUTPUT_PERCENTTAGE);

    list *l_key = listCreate();
    list *l_field = listCreate();

    dictIterator *di_key = dictGetIterator(db->rock_hash);
    dictEntry *de_key;
    while ((de_key = dictNext(di_key)))
    {
        const sds key = dictGetKey(de_key);
        dict *lrus = dictGetVal(de_key);

        dictIterator *di_field = dictGetIterator(lrus);
        dictEntry *de_field;
        while ((de_field = dictNext(di_field)))
        {
            const sds field = dictGetKey(de_field);

            listAddNodeTail(l_key, key);
            listAddNodeTail(l_field, field);
        }
        dictReleaseIterator(di_field);
        
    }
    dictReleaseIterator(di_key);

    listIter li_key;
    listNode *ln_key;
    listRewind(l_key, &li_key);

    listIter li_field;
    listNode *ln_field;
    listRewind(l_field, &li_field);

    const size_t total = db->rock_hash_field_cnt;
    const size_t scope = total / EVICT_OUTPUT_PERCENTTAGE;
    serverAssert(scope > 0);
    for (int i = 1; i <= EVICT_OUTPUT_PERCENTTAGE; ++i)
    {
        if (i != EVICT_OUTPUT_PERCENTTAGE)
        {
            rock_all_for_hash_for_db_by_one_percentage(c, dbid, i, scope, 
                                                       &li_key, &ln_key, &li_field, &ln_field);
        }
        else
        {
            const size_t last_scope = total - scope * (EVICT_OUTPUT_PERCENTTAGE-1);
            serverAssert(last_scope > 0);
            rock_all_for_hash_for_db_by_one_percentage(c, dbid, i, last_scope, 
                                                       &li_key, &ln_key, &li_field, &ln_field);            
        }
    }

    listRelease(l_key);
    listRelease(l_field);
}

static void rock_all_for_evict(client *c)
{
    int all_empty = 1;

    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;
        size_t num = dictSize(db->rock_evict);
        if (num == 0)
            continue;

        all_empty = 0;

        // This db head line for this db
        sds s = sdsempty();
        s = sdscatprintf(s, "key evict for dbid = %d, number = %zu", i, num);

        if (!server.cluster_enabled)
            addReplyBulkCString(c, s);

        sdsfree(s);

        if (num < EVICT_OUTPUT_PERCENTTAGE)
        {
            // one line output for small db
            rock_all_for_evict_for_db(i);
        }
        else
        {
            // 100 lines output for big db
            rock_all_for_evict_for_db_by_percentage(c, i);
        }
    }

    if (all_empty)
    {
        sds s = sdsempty();
        s = sdscatprintf(s, "All db key eviction are empty!");

        if (!server.cluster_enabled)
            addReplyBulkCString(c, s);

        sdsfree(s);
    }
}

static void rock_all_for_hash(client *c)
{
    int all_empty = 1;

    for (int i = 0; i < server.dbnum; ++i)
    {
        redisDb *db = server.db + i;
        size_t num = db->rock_hash_field_cnt;
        if (num == 0)
            continue;

        all_empty = 0;

        // This db head line for this db
        sds s = sdsempty();
        s = sdscatprintf(s, "field evict for dbid = %d, number = %zu", i, num);

        if (!server.cluster_enabled)
            addReplyBulkCString(c, s);

        sdsfree(s);

        if (num < EVICT_OUTPUT_PERCENTTAGE)
        {
            // one line output for small db
            rock_all_for_hash_for_db(i);
        }
        else
        {
            // 100 lines output for big db
            rock_all_for_hash_for_db_by_percentage(c, i);
        }
    }

    if (all_empty)
    {
        sds s = sdsempty();
        s = sdscatprintf(s, "All db field eviction are empty!");

        if (!server.cluster_enabled)
            addReplyBulkCString(c, s);

        sdsfree(s);
    }
}


void rock_all(client *c)
{
    int total = 0;
    total += cal_rock_all_for_evict();
    total += cal_rock_all_for_hash();

    if (!server.cluster_enabled)
        addReplyArrayLen(c, total);

    rock_all_for_evict(c);
    rock_all_for_hash(c);

    if (server.cluster_enabled)
        addReply(c, shared.ok);
}

/* Check current free memory is OK for continue the command.
 * Return 1 if OK, otherwise 0.
 *
 * If free memory is not enough, two conditions will return 0
 * 1. the client is in multi state and the command is to be buffered
 * 2. the command will resume memory, i.e., is_denyoom_command is true.
 *    For read-only command, it is OK even the value may be read from RocksDB
 *    and consume memory because we have some sserver.leastfreemem for safe guarantee.
 */
int check_free_mem_for_command(const client *c, const int is_denyoom_command)
{
    const long long least_free_mem = get_least_free_mem_in_bytes();
    if (least_free_mem < 0)
        return 1;       // disalbe least free mem check

    const size_t current_free_mem = get_free_mem_of_os();   // NOTE: for MacOs, it is the MAX_SIZE
    if (current_free_mem >= (size_t)least_free_mem)
        return 1;

    // free memory is not engough
    if (c->flags & CLIENT_MULTI &&
        c->cmd->proc != execCommand &&
        c->cmd->proc != discardCommand &&
        c->cmd->proc != resetCommand) 
        return 0;       // if in multi state and need to buffer the command, it is NOT OK

    return !is_denyoom_command;     // deny oom command
}

/* return size in bytes, if error, return 0.
 */
static size_t parse_rock_mem_size(const sds s)
{
    size_t len = sdslen(s);
    char last_char = s[len-1];
    if (!(last_char == 'm' || last_char == 'M' || last_char == 'g' || last_char == 'G'))
        return 0;
    
    long long parse_val = 0LL;
    if (string2ll(s, len-1, &parse_val) == 0)
        return 0;

    if (parse_val <= 0)
        return 0;

    const size_t val = (size_t)parse_val;

    if (last_char == 'g' || last_char == 'G')
    {
        if (SIZE_MAX/(1ULL<<30) < val)
            return 0;
        
        const size_t sz = val * (1ULL<<30);
        if (sz >= server.system_memory_size)
            return 0;

        return sz;
    }
    else
    {
        if (SIZE_MAX/(1ULL<<20) < val)
            return 0;

        const size_t sz = val * (1ULL<<20);
        if (sz >= server.system_memory_size)
            return 0;

        return sz;
    }
}

/* return timeout for seconds, if error, return 0.
 */
static size_t parse_rock_mem_timeout(const sds s)
{
    long long val;
    if (string2ll(s, sdslen(s), &val) == 0)
        return 0;
    
    if (val <= 0)
        return 0;

    return (size_t)val;
}

/* command rockmem <mem_size> <timeout>(optional)
 */
void rock_mem(client *c)
{
    void bytesToHuman(char *s, unsigned long long n);   // declaration in server.c

    // parse mem_size
    const size_t mem_size = parse_rock_mem_size(c->argv[1]->ptr);
    if (mem_size == 0)
    {
        addReplyError(c, "rockmem must specify correct memory size, not zero or negative to too large (more than system ram), the size number is like 77M or 77m or 77G or 77g");
        return;
    }

    size_t timeout_sec = SIZE_MAX;
    if (c->argc >= 3)
    {
        // parse timeout seconds
        timeout_sec = parse_rock_mem_timeout(c->argv[2]->ptr);
        if (timeout_sec == 0)
        {
            addReplyError(c, "rockmem timeout for seconds must be positive integer!");
            return;
        }
    }

    size_t timeout_ms;
    if (timeout_sec >= SIZE_MAX/1000)
    {
        timeout_ms = SIZE_MAX;
    }
    else
    {
        timeout_ms = timeout_sec*1000;
    }

    const size_t freed = perform_rock_eviction_for_rock_mem(mem_size, timeout_ms);
    
    sds result = sdsempty();
    char hmem[64];
    bytesToHuman(hmem, freed);
    result = sdscatprintf(result, "rockmem acutally freed %zu(bytes), which is %s", freed, hmem);

    addReplyBulkSds(c, result);
}
