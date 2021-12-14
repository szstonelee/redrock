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

#include <ftw.h>

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
void init_rocksdb(const char* folder_path)
{
    // verify last char, must be '/'
    const size_t path_len = strlen(folder_path);
    if (folder_path[path_len-1] != '/')
    {
        serverLog(LL_WARNING, "RocksDB folder path must be ended of slash char of /");
        exit(1);
    }

    // nftw(folder_path, unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
    nftw(folder_path, unlink_cb, USE_FDS, FTW_DEPTH | FTW_PHYS);
    serverLog(LL_NOTICE, "finish removal of the whole RocksDB folder = %s", folder_path);
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
            if (dictFind(c->db->dict, input_key))
            {
                serverLog(LL_NOTICE, "debug evictkeys, try key = %s", input_key);
                keys[len] = input_key;
                dbids[len] = c->db->id;
                ++len;
            }
        }
        int ecvict_num = try_evict_to_rocksdb(len, dbids, keys);
        serverLog(LL_NOTICE, "debug evictkeys, ecvict_num = %d", ecvict_num);
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
    
}