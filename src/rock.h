/*
 * Copyright (c) 2020-, szstonelee <szstonelee at vip qq com>
 * All rights reserved.
 * 
 * mainly based on Redis & Rocksdb, please check their rights
 * redis: https://github.com/antirez/redis/blob/unstable/COPYING
 * rocksdb: https://github.com/facebook/rocksdb/blob/master/COPYING
 * and other 3rd libaries:
 * snappy: https://github.com/google/snappy/blob/master/COPYING
 * lz4: https://github.com/lz4/lz4/blob/dev/LICENSE
 * bzip2: https://sourceware.org/git/bzip2.git
 * zstd: https://github.com/facebook/zstd/blob/dev/COPYING
 * zlib: http://zlib.net/ 
 * jemalloc: http://jemalloc.net/
 * lua: http://www.lua.org/license.html
 * hiredis: https://github.com/redis/hiredis
 * linenoise: check Readme.markdown in deps/linenoise 
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

#ifndef __ROCK_H
#define __ROCK_H

/* Map object types to Rocksdb object types. Macros starting with OBJ_ are for
 * memory storage and may change. Instead Rocksdb types must be fixed because
 * we store them on disk. */
#define ROCK_TYPE_STRING 0
// #define ROCK_TYPE_LIST   1
#define ROCK_TYPE_SET_HT    2
// #define ROCK_TYPE_ZSET   3
#define ROCK_TYPE_HASH_HT   4
#define ROCK_TYPE_ZSET_SKIPLIST 5 /* ZSET version 2 with doubles stored in binary. */

/* Object types for encoded objects. */
// #define ROCK_TYPE_HASH_ZIPMAP    9
// #define ROCK_TYPE_LIST_ZIPLIST  10
#define ROCK_TYPE_SET_INTSET    11
#define ROCK_TYPE_ZSET_ZIPLIST  12
#define ROCK_TYPE_HASH_ZIPLIST  13
#define ROCK_TYPE_LIST_QUICKLIST 14

#if defined(__APPLE__)
    #include <os/lock.h>
    static os_unfair_lock spinLock;
    // static os_unfair_lock spinLock = OS_UNFAIR_LOCK_INIT;
    #define rocklock() os_unfair_lock_lock(&spinLock)
    #define rockunlock() os_unfair_lock_unlock(&spinLock)

#else
    #include <pthread.h>
    static pthread_spinlock_t spinLock;
    #define rocklock() pthread_spin_lock(&spinLock)
    #define rockunlock() pthread_spin_unlock(&spinLock)
#endif

/* API */
void checkCallValueInRock(client *c);
void initRockPipe();
void initZeroRockJob();
void releaseRockKeyWhenFreeClient(client *c);
size_t getMemoryOfRock();
robj* loadValFromRockForRdb(int dbid, sds key);

int initRocksdb(int dbnum, char *path);
void teardownRocksdb();

size_t getMemoryOfRock();
void initSpinLock();

int isRockFeatureEnabled();

void checkRockForSingleCmd(client *c, list *l);
void checkRockForMultiCmd(client *c, list *l);
void checkThenResumeRockClient(client *c);
void doRockRestoreInMainThread(int dbid, sds key, robj **val);

void dumpValToRock(sds key, int dbid);

#endif
