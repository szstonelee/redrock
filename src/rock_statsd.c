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

#include "rock_statsd.h"
#include "server.h"
#include "rock.h"

#include <arpa/inet.h>
#include <sys/socket.h>

// file descriptor of the client upd socket
static int statsd_sock = -1;        
static struct sockaddr_in statsd_address;

// the config set/get buffer
char *statsd_config;
// the parse result of statsd_config
static const char *config_ip = NULL;
static size_t config_ip_len = 0;
static const char *config_port = NULL;
static size_t config_port_len = 0;
static const char *config_prefix = NULL;
static size_t config_prefix_len = 0;

static void set_server_address_port()
{
    memset(&statsd_address, 0, sizeof(statsd_address));
    statsd_address.sin_family = AF_INET;
    sds ip = sdsnewlen(config_ip, config_ip_len);
    statsd_address.sin_addr.s_addr = inet_addr(ip);
    serverLog(LL_WARNING, "set_server_address_port() ip = %s", ip);
    sdsfree(ip);
    sds port = sdsnewlen(config_port, config_port_len);
    statsd_address.sin_port = htons(atoi(port));
    serverLog(LL_WARNING, "set_server_address_port() port = %s", port);
    sdsfree(port);
}

static void init_config_empty()
{
    config_ip = NULL;
    config_ip_len = 0;
    config_port = NULL;
    config_port_len = 0;
    config_prefix = NULL;
    config_prefix_len = 0;
}

static int parse_ip(const char *start, const size_t max_len)
{
    size_t dot_cnt = 0;
    const char *p = start;
    size_t len = 0;

    for (size_t i = 0; i < max_len; ++i, ++p, ++len)
    {
        const char c = *p;

        if (c == 0)
            break;

        if (c == ':')
            break;

        if (c == '.')
        {
            if (i == 0) return 0;
            if (*(p-1) == '.') return 0;
            ++dot_cnt;
        }
        else
        {
            if (!(c >= '0' && c <= '9'))
                return 0;
        }
    }

    if (dot_cnt != 3)
        return 0;

    config_ip = start;
    config_ip_len = len;
    return 1;
}

static int parse_port(const size_t max_len)
{
    const char *p = config_ip + config_ip_len + 1;
    size_t len = 0;

    for (size_t i = 0; i < max_len; ++i, ++p, ++len)
    {
        const char c = *p;

        if (c == 0)
            break;
        
        if (c == ':')
            break;
        
        if (!(c >= '0' && c <= '9'))
            return 0;
    }

    if (len == 0)
        return 0;

    config_port = config_ip + config_ip_len + 1;
    config_port_len = len;
    return 1;
}

static int parse_prefix(const size_t max_len)
{
    if (max_len == config_ip_len + 1 + config_port_len)
    {
        // no prefix
        config_prefix = NULL;
        config_prefix_len = 0;
        return 1;
    }

    const char *p = config_ip + config_ip_len + 1 + config_port_len;
    if (*p != ':')
        return 0;

    if (max_len == config_ip_len + 1 + config_port_len + 1)
        return 0;       // can not be "127.0.0.1:8125:"

    config_prefix = p + 1;
    config_prefix_len = max_len - (config_ip_len + 1 + config_port_len + 1);
    return 1;
}


int is_valid_statsd_config(char *val, const char **err) {
    if (strcmp(val, "") == 0)
    {
        init_config_empty();
        return 1;
    }

    size_t max_len = strlen(val);
    *err = "statsd config example: config set 127.0.0.1:8125 or 127.0.0.1:8125:myprefix";

    const char *saved_config_ip = config_ip;
    const size_t saved_config_ip_len = config_ip_len;
    if (!parse_ip(val, max_len))
    {
        config_ip = saved_config_ip;
        config_ip_len = saved_config_ip_len;
        return 0;
    }

    const char *saved_config_port = config_port;
    const size_t saved_config_port_len = config_port_len;
    if (!parse_port(max_len))
    {
        config_ip = saved_config_ip;
        config_ip_len = saved_config_ip_len;
        config_port = saved_config_port;
        config_port_len = saved_config_port_len;
        return 0;
    }

    if (!parse_prefix(max_len))
        return 0;

    set_server_address_port();
    return 1;
}

void init_statsd()
{
    statsd_sock = socket(PF_INET, SOCK_DGRAM, 0);
    if (statsd_sock == -1)
        serverPanic("Error create UDP socket to Statsd server!");    
}

static void send_metric(const sds prefix, const char *fmt, const size_t val)
{
    static sds msg = NULL;

    if (msg == NULL)
    {
        msg = sdsempty();
    }
    else
    {
        sdsclear(msg);
    }

    msg = sdsdup(prefix);
    msg = sdscatfmt(msg, fmt, val);
    sendto(statsd_sock, msg, sdslen(msg), 0, (struct sockaddr*)&statsd_address, sizeof(statsd_address));
    
    // sdsfree(msg);
}

static void send_command_metric(const sds prefix, const char *fmt, const char *cmd_name, const size_t val)
{
    static sds msg = NULL;

    if (msg == NULL)
    {
        msg = sdsempty();
    }
    else
    {
        sdsclear(msg);
    }

    msg = sdsdup(prefix);
    msg = sdscatfmt(msg, fmt, cmd_name, val);
    sendto(statsd_sock, msg, sdslen(msg), 0, (struct sockaddr*)&statsd_address, sizeof(statsd_address));
}

void send_metrics_to_statsd_in_cron()
{
    if (config_ip == NULL)
        return;     // no config for statsd server

    // report to StatsD once for every second
    static int cron_cnt = 0;

    ++cron_cnt;
    if (cron_cnt != server.hz)
        return;

    cron_cnt = 0;
    
    // monotime timer;
    // elapsedStart(&timer);

    serverAssert(config_ip != NULL && config_port != NULL && 
                 config_ip_len != 0 && config_port_len != 0);

    sds prefix = sdsempty();
    if (config_prefix != NULL)
        prefix = sdscatlen(prefix, config_prefix, config_prefix_len);
        
    // free memory
    send_metric(prefix, ".FreeMem:%zu|g", get_free_mem_of_os());

    // used memory
    send_metric(prefix, ".UsedMem:%zu|g", zmalloc_used_memory());

    // rss
    send_metric(prefix, ".Rss:%zu|g", server.cron_malloc_stats.process_rss);

    // key info
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
    send_metric(prefix, ".TotalKey:%U|g", total_key_num);
    send_metric(prefix, ".TotalEvictKey:%U|g", total_rock_evict_num);
    send_metric(prefix, ".TotalDiskKey:%U|g", total_key_in_disk_num);
    send_metric(prefix, ".TotalHash:%U|g", total_rock_hash_num);
    send_metric(prefix, ".TotalHashField:%U|g", total_rock_hash_field_num);
    send_metric(prefix, ".TotalDiskField:%U|g", total_field_in_disk_num);

    // visit stats
    size_t key_total_visits, key_rock_visits, field_total_visits, field_rock_visits;
    get_visit_stat_for_rock(&key_total_visits, &key_rock_visits, &field_total_visits, &field_rock_visits);
    send_metric(prefix, ".KeyTotalVisits:%U|g", key_total_visits);
    send_metric(prefix, ".KeyDiskVisits:%U|g", key_rock_visits);
    send_metric(prefix, ".FieldTotalVisits:%U|g", field_total_visits);
    send_metric(prefix, ".FieldDiskVisits:%U|g", field_rock_visits);
    send_metric(prefix, ".KeySpaceHits:%U|g", server.stat_keyspace_hits);
    send_metric(prefix, ".KeySpaceMisses:%U|g", server.stat_keyspace_misses); 
    long long stat_total_reads_processed, stat_total_writes_processed;
    atomicGet(server.stat_total_reads_processed, stat_total_reads_processed);
    atomicGet(server.stat_total_writes_processed, stat_total_writes_processed);
    send_metric(prefix, ".TotalReads:%U|g", stat_total_reads_processed);      
    send_metric(prefix, ".TotalWrites:%U|g", stat_total_writes_processed);     

    // clients
    send_metric(prefix, ".ConnectedClients:%U|g", listLength(server.clients)-listLength(server.slaves));
    send_metric(prefix, ".MaxClients:%U|g", server.maxclients);
    send_metric(prefix, ".BlockedClients:%U|g", server.blocked_clients);

    // persistence    
    send_metric(prefix, ".RdbSaveSecs:%U|g", server.rdb_save_time_last);
    send_metric(prefix, ".RdbCowBytes:%U|g", server.stat_rdb_cow_bytes);
    send_metric(prefix, ".AofRewriteSecs:%U|g", server.aof_rewrite_time_last);
    send_metric(prefix, ".AofCowBytes:%U|g", server.stat_aof_cow_bytes);

    long long getInstantaneousMetric(int metric);   // declared in server.c
    // general stats
    send_metric(prefix, ".TotalReceivedConnections:%U|g", server.stat_numconnections);
    send_metric(prefix, ".TotalProcessedCommands:%U|g", server.stat_numcommands);
    send_metric(prefix, ".InstantOpsPerSecond:%U|g", getInstantaneousMetric(STATS_METRIC_COMMAND));   
    send_metric(prefix, ".LatestForkUsec:%U|g", server.stat_fork_time);

    // rocksdb stats
    send_metric(prefix, ".EstimateRocksdbDiskSize:%U|g", server.rocksdb_disk_size);
    send_metric(prefix, ".EstimateRocksdbKeyNumber:%U|g", server.rocksdb_key_num);

    // Command stats
    struct redisCommand *c;
    dictEntry *de;
    dictIterator *di;
    di = dictGetSafeIterator(server.commands);
    while((de = dictNext(di)) != NULL) 
    {
        char *tmpsafe;
        c = (struct redisCommand *) dictGetVal(de);
        if (!c->calls)
            continue;

        const char *s = getSafeInfoString(c->name, strlen(c->name), &tmpsafe);
        send_command_metric(prefix, ".cmds.%s.calls:%U|g", s, c->calls);
        send_command_metric(prefix, ".cmds.%s.usecPerCall:%U|g", s, (c->calls == 0) ? 0: c->microseconds/c->calls);
        if (tmpsafe != NULL) zfree(tmpsafe);
    }
    dictReleaseIterator(di);

    sdsfree(prefix);

    // const uint64_t elapse_us = elapsedUs(timer);
    // serverLog(LL_WARNING, "send_metrics_to_statsd(), time(us) = %zu", elapse_us);
}
