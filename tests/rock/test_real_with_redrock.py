#./redrock --port 7000 --cluster-enabled yes --cluster-config-file node7000.conf --cluster-node-timeout 5000 --save 0

#https://www.snel.com/support/how-to-install-grafana-graphite-and-statsd-on-ubuntu-18-04/

import redis
import random
import string
import time
import sys
import threading


r1: redis.StrictRedis   # redrock
r2: redis.StrictRedis   # real redis 6.2.2

r1_thread: redis.StrictRedis
r2_thread: redis.StrictRedis


def init_redis_clients():
    r1_ip = "192.168.64.4"
    r2_ip = "192.168.64.4"
    r1_port = 6379
    r2_port = 6380
    pool1 = redis.ConnectionPool(host=r1_ip,
                                 port=r1_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='latin1',
                                 socket_connect_timeout=2)
    pool2 = redis.ConnectionPool(host=r2_ip,
                                 port=r2_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='latin1',
                                 socket_connect_timeout=2)
    r1: redis.StrictRedis = redis.StrictRedis(connection_pool=pool1)
    r2: redis.StrictRedis = redis.StrictRedis(connection_pool=pool2)

    pool1_thread = redis.ConnectionPool(host=r1_ip,
                                        port=r1_port,
                                        db=0,
                                        decode_responses=True,
                                        encoding='latin1',
                                        socket_connect_timeout=2)
    pool2_thread = redis.ConnectionPool(host=r2_ip,
                                        port=r2_port,
                                        db=0,
                                        decode_responses=True,
                                        encoding='latin1',
                                        socket_connect_timeout=2)
    r1_thread: redis.StrictRedis = redis.StrictRedis(connection_pool=pool1_thread)
    r2_thread: redis.StrictRedis = redis.StrictRedis(connection_pool=pool2_thread)

    return r1, r2, r1_thread, r2_thread


def insert_6K_keys_for_redrock():
    print("starting insert 6k keys to RedRock so RedRock will use disk for this test...")
    for i in range(0, 1_000):
        if i % 100 == 0:
            print(f"insert_6K_keys_for_redrock(), hash first(field number = 5), i = {i}")
        k = "init_for_redrock_hash_" + str(i)
        field_v = "fv" * 500
        cmd = f"hmset {k} f1 {field_v} f2 {field_v} f3 {field_v} f4 {field_v} f5 {field_v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
    for i in range(0, 5_000):
        # then hash
        if i % 1000 == 0:
            print(f"insert_6K_keys_for_redrock(), string second, i = {i}")
        k = "init_for_redrcok_str_" + str(i)
        v = "v" * 1000
        cmd = f"set {k} {v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)

    print("insert_6K_keys_for_redrock finished!!!!")


def init_redrock(r: redis.StrictRedis):
    r.execute_command("config set hash-max-ziplist-entries 2")
    r.execute_command("config set hash-max-rock-entries 4")
    r.execute_command("config set maxrockmem 10000000")  # 10M
    r.execute_command("config set appendonly yes")
    dbsize = r.execute_command("dbsize")
    print(f"dbsize = {dbsize}")
    if dbsize < 5_000:
        insert_6K_keys_for_redrock()
    #r.execute_command("config set save '3600 1 300 100 60 10000'")


def get_key(key_prefix: str):
    key = key_prefix
    for _ in range(0, random.randint(1, 100)):
        key = key + random.choice(string.digits)
    return key


def get_fields(field_prefix: str):
    fields = set()
    for _ in range(0, 100):
        fields.add(field_prefix + str(random.randint(1, 1000)))
    return list(fields)


def get_keys(key_prefix: str):
    keys = []
    for _ in range(0, random.randint(1, 10)):
        key = key_prefix
        for _ in range(0, random.randint(1, 100)):
            key = key + random.choice(string.digits)
        keys.append(key)
    return keys


def get_val():
    val = ""
    for _ in range(0, random.randint(20, 2000)):
        val = val + random.choice(string.ascii_letters)
    return val


def get_int():
    return random.randint(0, 9999999)


def get_float():
    return random.randint(0, 9999999) + 0.1


def check(res1, res2, cmd_name, cmd):
    if res1 != res2:
        print(f"res1 = {res1}")
        print(f"res2 = {res2}")
        raise Exception(f"cmd_name = {cmd_name}, cmd = {cmd}")


def check_same(key: str, caller: str):
    cmd = f"exists {key}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    if res1 != res2:
        msg = f"check_compare fail for {key} exist. caller = {caller}"
        print(msg)
        raise Exception(msg)
    if not res1:
        return
    key_type = r1.execute_command(f"type {key}")
    if key_type == "set" or key_type == "hash":
        # check fiield number first
        if key_type == "hash":
            all_fields: list = r1.execute_command(f"hkeys {key}")
            r2_field_num = r2.execute_command(f"hlen {key}")
        else:
            all_fields: list = r1.execute_command(f"smembers {key}")
            r2_field_num = r2.execute_command(f"scard {key}")
        if len(all_fields) != r2_field_num:
            raise Exception(f"check_same() fail, field number not match, key = {key}, caller = {caller}")
        # check all fields
        for f in all_fields:
            if key_type == "hash":
                f_exist = r2.execute_command(f"hexists {key} {f}")
                if not f_exist:
                    raise Exception(f"check_same() fail for hash, field not exist, key = {key}, field = {f}, caller = {caller}")
            else:
                f_exist = r2.execute_command(f"sismember {key} {f}")
                if not f_exist:
                    raise Exception(f"check_same() fail for set, field not exist, key = {key}, field = {f}, caller = {caller}")
        if key_type == "hash":
            # check value
            for f in all_fields:
                cmd = f"hget {key} {f}"
                r1_val = r1.execute_command(cmd)
                r2_val = r2.execute_command(cmd)
                if r1_val != r2_val:
                    raise Exception(f"check_same() fail for hash, value not match, key = {key}, field = {f}, caller = {caller}")
    elif key_type == "list":
        cmd = f"llen {key}"
        len1 = r1.execute_command(cmd)
        len2 = r2.execute_command(cmd)
        if len1 != len2:
            raise Exception(f"check_same() fail for list, key = {key}, list len not same!")
        cmd = f"lrange {key} 0 -1"
        all1 = r1.execute_command(cmd)
        all2 = r2.execute_command(cmd)
        if all1 != all2:
            raise Exception(f"check_same() fail for list, key = {key}, list content not same!")
    else:
        cmd = f"dump {key}"
        res1 = r1.execute_command(cmd)
        res2 = r2.execute_command(cmd)
        if res1 != res2:
            msg = f"check_same() fail for {key} dump. caller = {caller}"
            print(msg)
            raise Exception(msg)


def delete_whole_key(k: str):
    cmd = f"del {k}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)


def string_set(name: str):
    k = get_key("strkey")
    v = get_val()
    cmd = f"set {k} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def strlen(name: str):
    k = get_key("strkey")
    cmd = f"strlen {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def append(name: str):
    k = get_key("strkey")
    exist = r2.execute_command(f"exists {k}")
    if not exist:
        return
    v = get_val()
    cmd = f"append {k} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def decr(name: str):
    k = get_key("strkey")
    v = get_int()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"decr {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def incr(name: str):
    k = get_key("strkey")
    v = get_int()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"incr {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def decrby(name: str):
    k = get_key("strkey")
    v1 = get_int()
    cmd = f"set {k} {v1}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    v2 = get_int()
    cmd = f"decrby {k} {v2}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def incrby(name: str):
    k = get_key("strkey")
    v1 = get_int()
    cmd = f"set {k} {v1}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    v2 = get_int()
    cmd = f"incrby {k} {v2}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def incrbyfloat(name: str):
    k = get_key("strkey")
    v1 = get_float()
    cmd = f"set {k} {v1}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    v2 = get_float()
    cmd = f"incrbyfloat {k} {v2}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def get(name: str):
    k = get_key("strkey")
    cmd = f"get {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def getdel(name: str):
    k = get_key("strkey")
    cmd = f"getdel {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def getex(name: str):
    k = get_key("strkey")
    cmd = f"getex {k} px 5"     # NOTE: if 10, maybe not correct, because of time accuracy
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    time.sleep(0.01)
    cmd = f"get {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def getrange(name: str):
    k = get_key("strkey")
    start = random.randint(0, 10)
    end = start + random.randint(0, 1000)
    cmd = f"getrange {k} {start} {end}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def getset(name: str):
    k = get_key("strkey")
    v = get_val()
    cmd = f"getset {k} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def mget(name: str):
    ks = get_keys("strkey")
    cmd = f"mget "
    for k in ks:
        cmd = cmd + " " + k
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def mset(name: str):
    ks = get_keys("strkey")
    cmd = f"mset "
    for k in ks:
        cmd = cmd + " " + k
        val = get_val()
        cmd = cmd + " " + val
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def msetnx(name: str):
    ks = get_keys("strkey")
    cmd = f"msetnx "
    for k in ks:
        cmd = cmd + " " + k
        val = get_val()
        cmd = cmd + " " + val
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def psetex(name: str):
    k = get_key("strkey")
    v = get_val()
    cmd = f"psetex {k} 5 {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    time.sleep(0.01)
    cmd = f"get {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def setex(name: str):
    k = get_key("strkey")
    v = get_val()
    cmd = f"setex {k} 1 {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    time.sleep(2)
    cmd = f"get {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def setnx(name: str):
    k = get_key("strkey")
    v = get_val()
    cmd = f"setnx {k} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def setrange(name: str):
    k = get_key("strkey")
    offset = random.randint(1, 100)
    v = get_val()
    cmd = f"setrange {k} {offset} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def substr(name: str):
    k = get_key("strkey")
    start = random.randint(1, 100)
    end = start + random.randint(1, 1000)
    cmd = f"substr {k} {start} {end}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def string_cmd_table():
    cmds: dict = {"set": string_set,
                  "append": append,
                  "decr": decr,
                  "decrby": decrby,
                  "get": get,
                  "getdel": getdel,
                  "getex": getex,
                  "getrange": getrange,
                  "getset": getset,
                  "incr": incr,
                  "incrby": incrby,
                  "incrbyfloat": incrbyfloat,
                  "mget": mget,
                  "mset": mset,
                  "msetnx": msetnx,
                  "psetex": psetex,
                  "setex": setex,
                  "setnx": setnx,
                  "setrange": setrange,
                  "strlen": strlen,
                  "substr": substr,
                  }
    return cmds


def lindex(name: str):
    k = get_key("listkey")
    for _ in range(0, random.randint(1, 100)):
        v = random.randint(0, 9999999)
        cmd = f"lpush {k} {v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
    index = random.randint(0, 120)
    cmd = f"lindex {k} {index}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def linsert(name: str):
    k = get_key("listkey")
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = f"rpush {k} {v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
    pivot = random.randint(100, 999)
    element = random.randint(0, 999999)
    cmd = f"linsert {k} before {pivot} {element}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def llen(name: str):
    k = get_key("listkey")
    cmd = f"llen {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lmove(name: str):
    src = get_key("listkey")
    dst = get_key("listkey")
    cmd = f"lmove {src} {dst} right left"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lpop(name: str):
    k = get_key("listkey")
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = f"rpush {k} {v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
    count = random.randint(1, 10)
    cmd = f"lpop {k} {count}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def rpop(name: str):
    k = get_key("listkey")
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = f"rpush {k} {v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
    count = random.randint(1, 10)
    cmd = f"rpop {k} {count}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lpos(name: str):
    k = get_key("listkey")
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = f"rpush {k} {v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
    element = random.randint(1, 999)
    cmd = f"lpos {k} {element}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lpush(name: str):
    k = get_key("listkey")
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = f"lpush {k} {v}"
        res1 = r1.execute_command(cmd)
        res2 = r2.execute_command(cmd)
        check(res1, res2, name, cmd)


def rpush(name: str):
    k = get_key("listkey")
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = f"rpush {k} {v}"
        res1 = r1.execute_command(cmd)
        res2 = r2.execute_command(cmd)
        check(res1, res2, name, cmd)


def lpushx(name: str):
    k = get_key("listkey")
    cmd = f"lpushx {k}"
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = cmd + " " + str(v)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def rpushx(name: str):
    k = get_key("listkey")
    cmd = f"rpushx {k}"
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = cmd + " " + str(v)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lrange(name: str):
    k = get_key("listkey")
    start = random.randint(0, 10)
    end = start + random.randint(5, 100)
    cmd = f"lrange {k} {start} {end}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lrem(name: str):
    k = get_key("listkey")
    element = random.randint(1, 999)
    cmd = f"lrem {k} -2 {element}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lset(name: str):
    k = get_key("listkey")
    cmd = f"lpush {k}"
    len_v = random.randint(1, 1000)
    for _ in range(0, len_v):
        v = random.randint(0, 999999)
        cmd = cmd + " " + str(v)
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    index = random.randint(0, len_v - 1)
    element = random.randint(1, 999)
    cmd = f"lset {k} {index} {element}"
    try:
        res1 = r1.execute_command(cmd)
        res2 = r2.execute_command(cmd)
        check(res1, res2, name, cmd)
    except redis.exceptions.ResponseError:
        print(cmd)
        exit()


def ltrim(name: str):
    k = get_key("listkey")
    cmd = f"lpush {k}"
    len = random.randint(1, 1000)
    for _ in range(0, len):
        v = random.randint(0, 999999)
        cmd = cmd + " " + str(v)
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    start = random.randint(0, 10)
    stop = start + random.randint(5, 100)
    cmd = f"ltrim {k} {start} {stop}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def rpoplpush(name: str):
    src = get_key("listkey")
    dst = get_key("listkey")
    cmd = f"rpoplpush {src} {dst}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def list_cmd_table():
    cmds: dict = {"lindex": lindex,
                  "linsert": linsert,
                  "llen": llen,
                  "lmove": lmove,
                  "lpop": lpop,
                  "lpos": lpos,
                  "lpush": lpush,
                  "lpushx": lpushx,
                  "lrange": lrange,
                  "lrem": lrem,
                  "lset": lset,
                  "ltrim": ltrim,
                  "rpop": rpop,
                  "rpoplpush": rpoplpush,
                  "rpush": rpush,
                  "rpushx": rpushx,
                  }
    return cmds


def bitcount(name: str):
    k = get_key("bckey")
    v = get_val()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    start = random.randint(-100, 100)
    end = random.randint(-100, 100)
    cmd = f"bitcount {k} {start} {end}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def bitfield(name: str):
    k = get_key("bckey")
    v = get_val()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"BITFIELD {k} INCRBY i5 100 1 GET u4 0"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"BITFIELD {k} SET i8 #0 100 SET i8 #1 200"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    check_same(k, "bitfield")


def bitfield_ro(name: str):
    k = get_key("bckey")
    v = get_val()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"bitfield_ro {k} GET i8 16"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    check_same(k, "bitfield_ro")


def bitop(name: str):
    k1 = get_key("bckey1")
    v1 = get_val()
    k2 = get_key("bckey2")
    v2 = get_val()
    dst = get_key("bckeydest")
    cmd = f"bitop AND {dst} {k1} {k2}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(dst, "bitop_AND")
    cmd = f"bitop OR {dst} {k1} {k2}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(dst, "bitop_OR")
    cmd = f"bitop XOR {dst} {k1} {k2}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(dst, "bitop_XOR")
    cmd = f"bitop NOT {dst} {k1}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(dst, "bitop_NOT")


def bitpos(name: str):
    k = get_key("bckey")
    v = get_val()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    start = random.randint(-100, 100)
    end = random.randint(-100, 100)
    cmd = f"bitpos {k} 1 {start} {end}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def getbit(name: str):
    k = get_key("bckey")
    v = get_val()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    offset = random.randint(0, 100)
    cmd = f"getbit {k} {offset}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def setbit(name:str):
    k = get_key("bckey")
    v = get_val()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    offset = random.randint(0, 100)
    cmd = f"setbit {k} {offset} 0"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(k, "setbit")


def bitmap_cmd_table():
    cmds: dict = {"bitcount": bitcount,
                  "bitfield": bitfield,
                  "bitfield_ro": bitfield_ro,
                  "bitop": bitop,
                  "bitpos": bitpos,
                  "getbit": getbit,
                  "setbit": setbit,
                  }
    return cmds


def hash_insert_one():
    k = get_key("hashkey")
    cmd = f"del {k}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    fs = get_fields("f")
    for f in fs:
        v = get_val()
        cmd = f"hset {k} {f} {v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
    return k, fs


def hdel(name: str):
    k, fs = hash_insert_one()
    f1 = random.choice(fs)
    f2 = random.choice(fs)
    f3 = random.choice(fs)
    cmd = f"hdel {k} {f1} {f2} {f3}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hexists(name: str):
    k, fs = hash_insert_one()
    f = random.choice(fs)
    cmd = f"hexists {k} {f}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hget(name: str):
    k, fs = hash_insert_one()
    f = random.choice(fs)
    cmd = f"hget {k} {f}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hgetall(name: str):
    k, fs = hash_insert_one()
    cmd = f"hgetall {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    if 2 * len(fs) != len(res1):
        print(fs)
        raise Exception(f"hgetall, key = {k}, 2 * len(fs) != len(res1), 2 * len(fs) = {2 * len(fs)}, len(res1) = {len(res1)}")
    if 2 * len(fs) != len(res2):
        raise Exception(f"hgetall key = {k}, len(fs) != len(res2), 2 * len(fs) = {2 * len(fs)}, len(res2) = {len(res1)}")
    for f in fs:
        if f not in res1:
            raise Exception(f"hgetall key = {k}, f not in res1")
        if f not in res2:
            raise Exception(f"hgetall key = {k}, f not in res2")


def hincrby(name: str):
    k, fs = hash_insert_one()
    f = random.choice(fs)
    n = random.randint(-1000_000, 1000_000)
    cmd = f"hset {k} {f} {n}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    incr = random.randint(-100, 100)
    cmd = f"hincrby {k} {f} {incr}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    if res1 != n + incr:
        raise Exception(f"hincrby for res1 key = {k}, field = {f}, n = {n}, incr = {incr}")
    if res2 != n + incr:
        raise Exception(f"hincrby for res2 key = {k}, field = {f}, n = {n}, incr = {incr}")


def hincrbyflat(name: str):
    k, fs = hash_insert_one()
    f = random.choice(fs)
    n = random.randint(-1000_000, 1000_000)
    cmd = f"hset {k} {f} {n}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    incr = 10.1
    cmd = f"hicrby {k} {f} {incr}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hkeys(name: str):
    k, fs = hash_insert_one()
    cmd = f"hkeys {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hlen(name: str):
    k, fs = hash_insert_one()
    cmd = f"hlen {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    if res1 != len(fs) or res2 != len(fs):
        raise Exception(f"hlen, key = {k}")


def hmget(name: str):
    k, fs = hash_insert_one()
    f1 = random.choice(fs)
    f2 = random.choice(fs)
    f3 = random.choice(fs)
    cmd = f"hmget {k} {f1} {f2} {f3}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hmset(name: str):
    k = get_key("hashkey")
    cmd = f"del {k}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"hmset {k} f1 v1 f2 v2"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hrandfield(name: str):
    k, fs = hash_insert_one()
    res1 = r1.execute_command(f"hrandfield {k} 3")
    for key in res1:
        if key not in fs:
            raise Exception(f"hrandfield, key = {k}")


def hset(name: str):
    k = get_key("hashkey")
    cmd = f"del {k}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"hset {k} f1 v1 f2 v2 f3 v3"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hsetnx(name: str):
    k, fs = hash_insert_one()
    f = random.choice(fs)
    cmd = f"hsetnx {k} {f} v"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    new_f = get_key("random")
    cmd = f"hsetnx {k} {new_f} vv"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hstrlen(name: str):
    k, fs = hash_insert_one()
    f = random.choice(fs)
    cmd = f"hstrlen {k} {f}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hvals(name: str):
    k, _ = hash_insert_one()
    cmd = f"hvals {k}"
    res1: list = r1.execute_command(cmd)
    res2: list = r2.execute_command(cmd)
    res1.sort()
    res2.sort()
    check(res1, res2, name, cmd)


def hash_cmd_table():
    cmds: dict = {"hdel": hdel,
                  "hexists": hexists,
                  "hget": hget,
                  "hgetall": hgetall,
                  "hincrby": hincrby,
                  "hlen": hlen,
                  "hmget": hmget,
                  "hmset": hmset,
                  "hrandfield": hrandfield,
                  "hset": hset,
                  "hsetnx": hsetnx,
                  "hstrlen": hstrlen,
                  "hvals": hvals,
                  }
    return cmds


def add_set_key(k: str):
    cmd = f"sadd {k} "
    members = []
    for _ in range(0, random.randint(1, 100)):
        member = get_key("m_")
        cmd = cmd + member
        members.append(member)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    return res1, res2, cmd, members


def sadd(name: str):
    k = get_key("setkey")
    res1, res2, cmd, _ = add_set_key(k)
    check(res1, res2, name, cmd)


def scard(name: str):
    k = get_key("setkey")
    add_set_key(k)
    cmd = f"scard {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def sdiff(name: str):
    k1 = get_key("setkey")
    add_set_key(k1)
    k2 = get_key("setkey")
    add_set_key(k2)
    k3 = get_key("setkey")
    add_set_key(k3)
    cmd = f"sdiff {k1} {k2} {k3}"
    res1:list = r1.execute_command(cmd)
    res1.sort()
    res2:list = r2.execute_command(cmd)
    res2.sort()
    check(res1, res2, name, cmd)


def sdiffstore(name: str):
    k1 = get_key("setkey")
    add_set_key(k1)
    k2 = get_key("setkey")
    add_set_key(k2)
    dst = get_key("setkey")
    if dst in (k1, k2):
        dst = get_key("setkey")
    cmd = f"sdiffstore {dst} {k1} {k2}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def sinter(name: str):
    k1 = get_key("setkey")
    add_set_key(k1)
    k2 = get_key("setkey")
    add_set_key(k2)
    cmd = f"sinter {k1} {k2}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def sinterstore(name: str):
    k1 = get_key("setkey")
    add_set_key(k1)
    k2 = get_key("setkey")
    add_set_key(k2)
    dst = get_key("setkey")
    if dst in (k1, k2):
        dst = get_key("setkey")
    cmd = f"sinterstore {dst} {k1} {k2}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def sismember(name: str):
    k = get_key("setkey")
    add_set_key(k)
    for _ in range(0, 100):
        member = get_key("m_")
        cmd = f"sismember {k} {member}"
        res1 = r1.execute_command(cmd)
        res2 = r2.execute_command(cmd)
        check(res1, res2, name, cmd)


def smembers(name: str):
    k = get_key("setkey")
    add_set_key(k)
    cmd = f"smembers {k}"
    res1: list = r1.execute_command(cmd)
    res2: list = r2.execute_command(cmd)
    if res1.sort() != res2.sort():
        raise Exception(f"smembers key = {k}")


def smismember(name: str):
    k = get_key("setkey")
    add_set_key(k)
    members = ""
    for _ in range(0, 100):
        member = get_key("m_")
        members = members + " " + member
    cmd = f"smismember {k} " + members
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def smove(name: str):
    k = get_key("setkey")
    _, _, _, members = add_set_key(k)
    member = random.choice(members)
    dst = get_key("setkey")
    if dst == k:
        dst = get_key("setkey")
    cmd = f"smove {k} {dst} {member}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def spop(name: str):
    k = get_key("setkey")
    add_set_key(k)
    cmd = f"spop {k}"
    res1 = r1.execute_command(cmd)
    cmd = f"sismember {k} {res1}"
    res2 = r2.execute_command(cmd)
    if not res2:
        print(f"res1 = {res1}")
        raise Exception(f"spop key = {k}")
    delete_whole_key(k)


def srandmember(name: str):
    k = get_key("setkey")
    add_set_key(k)
    cmd = f"srandmember {k}"
    res1 = r1.execute_command(cmd)
    cmd = f"sismember {k} {res1}"
    res2 = r2.execute_command(cmd)
    if not res2:
        print(f"res1 = {res1}")
        raise Exception(f"srandmember key = {k}")


def srem(name: str):
    k = get_key("setkey")
    _, _, _, members = add_set_key(k)
    to_removes = []
    for _ in range(0, 10):
        to_removes.append(random.choice(members))
    cmd = f"srem {k}"
    for to_rem in to_removes:
        cmd = cmd + " " + to_rem
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def sunion(name: str):
    k1 = get_key("setkey")
    add_set_key(k1)
    k2 = get_key("setkey")
    add_set_key(k2)
    cmd = f"sunion {k1} {k2}"
    res1: list = r1.execute_command(cmd)
    res2: list = r2.execute_command(cmd)
    if res1.sort() != res2.sort():
        raise Exception(f"sunion, key1 = {k1}, key2 = {k2}")


def sunionstore(name: str):
    k1 = get_key("setkey")
    add_set_key(k1)
    k2 = get_key("setkey")
    add_set_key(k2)
    dst = get_key("setkey")
    if dst in (k1, k2):
        dst = get_key("setkey")
    cmd = f"sunionstore {dst} {k1} {k2}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def set_cmd_table():
    cmds: dict = {"sadd": sadd,
                  "scard": scard,
                  "sdiff": sdiff,
                  "sdiffstore": sdiffstore,
                  "sinter": sinter,
                  "sinterstore": sinterstore,
                  "sismember": sismember,
                  "smembers": smembers,
                  "smismember": smismember,
                  "smove": smove,
                  "spop": spop,
                  "srandmember": srandmember,
                  "srem": srem,
                  "sunion": sunion,
                  "sunionstore": sunionstore,
                  }
    return cmds


def zset_insert_one():
    k = get_key("zsetkey")
    cmd = f"del {k}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    members = get_fields("z")
    zset = {}
    for m in members:
        s = random.randint(-1000_000, 1000_000)
        cmd = f"zadd {k} {s} {m}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
        zset[m] = s
    return k, zset


def thread_insert_one_member(k: str):
    time.sleep(1)
    m = get_key("member")
    s = random.randint(-100, 100)
    cmd = f"zadd {k} {s} {m}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)


def bzpopmax(name: str):
    k = get_key("zsetkey")
    delete_whole_key(k)
    t = threading.Thread(target=thread_insert_one_member, args=(k,))
    t.start()
    cmd = f"bzpopmax {k} 0"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    k, _ = zset_insert_one()
    cmd = f"bzpopmax {k} 1"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def bzpopmin(name: str):
    k = get_key("zsetkey")
    delete_whole_key(k)
    t = threading.Thread(target=thread_insert_one_member, args=(k,))
    t.start()
    cmd = f"bzpopmin {k} 0"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    k, _ = zset_insert_one()
    cmd = f"bzpopmin {k} 1"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zadd(name: str):
    k = get_key("zsetkey")
    delete_whole_key(k)
    s = random.randint(-1000_000, 1000_000)
    m = get_key("member")
    cmd = f"zadd {k} {s} {m}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zcard(name: str):
    k, _ = zset_insert_one()
    cmd = f"zcard {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zcount(name: str):
    k, _ = zset_insert_one()
    min = random.randint(-100_000, 100_000)
    max = random.randint(-100_000, 100_000)
    if min > max:
        min, max = max, min
    cmd = f"zcount {k} {min} {max}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zdiff(name: str):
    k1, _ = zset_insert_one()
    k2, _ = zset_insert_one()
    k3, _ = zset_insert_one()
    cmd = f"zdiff 3 {k1} {k2} {k3} WITHSCORES"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zdiffstore(name: str):
    k1, _ = zset_insert_one()
    k2, _ = zset_insert_one()
    k3, _ = zset_insert_one()
    d = get_key("zsetkey")
    if d in (k1, k2, k3):
        d = get_key("zsetkey")
    cmd = f"zdiffstore {d} 3 {k1} {k2} {k3}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zincrby(name: str):
    k, zset = zset_insert_one()
    m = random.choice(list(zset))
    incr = random.randint(-100, 100)
    cmd = f"zincrby {k} {incr} {m}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zinter(name: str):
    k1, _ = zset_insert_one()
    k2, _ = zset_insert_one()
    k3, _ = zset_insert_one()
    cmd = f"zinter 3 {k1} {k2} {k3} AGGREGATE SUM WITHSCORES"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zinterstore(name: str):
    k1, _ = zset_insert_one()
    k2, _ = zset_insert_one()
    k3, _ = zset_insert_one()
    d = get_key("zsetkey")
    if d in (k1, k2, k3):
        d = get_key("zsetkey")
    cmd = f"zinterstore {d} 3 {k1} {k2} {k3} AGGREGATE SUM"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zlexcount(name: str):
    k, _ = zset_insert_one()
    cmd = f"zlexcount {k} - +"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zmscore(name: str):
    k, zset = zset_insert_one()
    m1 = random.choice(list(zset))
    m2 = random.choice(list(zset))
    cmd = f"zmscore {k} {m1} {m2}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zpopmax(name: str):
    k, _ = zset_insert_one()
    cmd = f"zpopmax {k} 2"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zpopmin(name: str):
    k, _ = zset_insert_one()
    cmd = f"zpopmin {k} 2"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrange(name: str):
    k, _ = zset_insert_one()
    min = random.randint(-100_000, 100_000)
    max = random.randint(-100_000, 100_000)
    if min > max:
        min, max = max, min
    cmd = f"zrange {k} {min} {max}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrangebylex(name: str):
    k, _ = zset_insert_one()
    cmd = f"zrangebylex {k} - +"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrangebyscore(name: str):
    k, _ = zset_insert_one()
    min = random.randint(-100_000, 100_000)
    max = random.randint(-100_000, 100_000)
    if min > max:
        min, max = max, min
    cmd = f"zrangebyscore {k} {min} {max}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrangestore(name: str):
    k, _ = zset_insert_one()
    min = random.randint(-100_000, 100_000)
    max = random.randint(-100_000, 100_000)
    if min > max:
        min, max = max, min
    d = get_key("zsetkey")
    if d == k:
        d = get_key("zsetkey")
    cmd = f"zrangestore {d} {k} {min} {max}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrank(name: str):
    k, zset = zset_insert_one()
    m = random.choice(list(zset))
    cmd = f"zrank {k} {m}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrem(name: str):
    k, zset = zset_insert_one()
    m = random.choice(list(zset))
    m_maybe = get_key("member")
    cmd = f"zrem {k} {m} {m_maybe}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zremrangebylex(name: str):
    k, _ = zset_insert_one()
    cmd = f"zremrangebylex {k} [alpha [omega"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zremrangebyrank(name: str):
    k, _ = zset_insert_one()
    start = random.randint(0, 10)
    stop = start + random.randint(1, 3)
    cmd = f"zremrangebyrank {k} {start} {stop}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zremrangebyscore(name: str):
    k, _ = zset_insert_one()
    min = random.randint(-100_000, 100_000)
    max = random.randint(-100_000, 100_000)
    if min > max:
        min, max = max, min
    cmd = f"zremrangebyscore {k} {min} {max}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrevrange(name: str):
    k, _ = zset_insert_one()
    start = random.randint(0, 10)
    stop = start + random.randint(1, 3)
    cmd = f"zrevrange {k} {start} {stop}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrevrangebylex(name: str):
    k, _ = zset_insert_one()
    cmd = f"zrevrangebylex {k} (g [aaa"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrevrangebyscore(name: str):
    k, _ = zset_insert_one()
    min = random.randint(-100_000, 100_000)
    max = random.randint(-100_000, 100_000)
    if min > max:
        min, max = max, min
    cmd = f"zrevrangebyscore {k} {max} {min}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zrevrank(name: str):
    k, zset = zset_insert_one()
    m = random.choice(list(zset))
    cmd = f"zrevrank {k} {m}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zscore(name: str):
    k, zset = zset_insert_one()
    m = random.choice(list(zset))
    cmd = f"zscore {k} {m}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zunion(name: str):
    k1, _ = zset_insert_one()
    k2, _ = zset_insert_one()
    k3, _ = zset_insert_one()
    cmd = f"zunion 3 {k1} {k2} {k3} AGGREGATE SUM"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def zunionstore(name: str):
    k1, _ = zset_insert_one()
    k2, _ = zset_insert_one()
    k3, _ = zset_insert_one()
    d = get_key("zsetkey")
    if d in (k1, k2, k3):
        d = get_key("zsetkey")
    cmd = f"zunionstore {d} 3 {k1} {k2} {k3} AGGREGATE SUM"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)



def zset_cmd_table():
    cmds: dict = {"bzpopmax": bzpopmax,
                  "bzpopmin": bzpopmin,
                  "zadd": zadd,
                  "zcard": zcard,
                  "zcount": zcount,
                  "zdiff": zdiff,
                  "zdiffstore": zdiffstore,
                  "zincrby": zincrby,
                  "zinter": zinter,
                  "zinterstore": zinterstore,
                  "zlexcount": zlexcount,
                  "zmscore": zmscore,
                  "zpopmax": zpopmax,
                  "zpopmin": zpopmin,
                  "zrange": zrange,
                  "zrangebylex": zrangebylex,
                  "zrangebyscore": zrangebyscore,
                  "zrangestore": zrangestore,
                  "zrank": zrank,
                  "zrem": zrem,
                  "zremrangebylex": zremrangebylex,
                  "zremrangebyrank": zremrangebyrank,
                  "zremrangebyscore": zremrangebyscore,
                  "zrevrange": zrevrange,
                  "zrevrangebylex": zrevrangebylex,
                  "zrevrangebyscore": zrevrangebyscore,
                  "zrevrank": zrevrank,
                  "zscore": zscore,
                  "zunion": zunion,
                  "zunionstore": zunionstore,
                  }
    return cmds


def create_string_key(k: str):
    v = get_val()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)


def create_list_key(k: str):
    delete_whole_key(k)
    for _ in range(0, random.randint(1, 100)):
        ele = get_val()
        cmd = f"rpush {k} {ele}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)


def create_hash_key(k: str):
    delete_whole_key(k)
    fields = []
    for _ in range(0, random.randint(1, 100)):
        field = get_key("f")
        val = get_val()
        cmd = f"hset {k} {field} {val}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
        fields.append(field)
    return fields


def create_set_key(k: str):
    delete_whole_key(k)
    members = []
    for _ in range(0, random.randint(1, 100)):
        member = get_key("f")
        cmd = f"sadd {k} {member}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
        members.append(member)
    return members


def create_zset_key(k: str):
    delete_whole_key(k)
    ms = {}
    for _ in range(0, random.randint(1, 100)):
        member = get_key("m")
        score = random.randint(-1000_000, 1000_000)
        cmd = f"zadd {k} NX {score} {member}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
        ms[member] = score
    return ms


def copy(name: str):
    src = get_key("generickey")
    dst = get_key("generickey")
    create_string_key(src)
    cmd = f"copy {src} {dst} REPLACE"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_list_key(src)
    cmd = f"copy {src} {dst} REPLACE"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_hash_key(src)
    cmd = f"copy {src} {dst} REPLACE"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_set_key(src)
    cmd = f"copy {src} {dst} REPLACE"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_zset_key(src)
    cmd = f"copy {src} {dst} REPLACE"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def del_redis(name):
    k1 = get_key("generickey")
    k2 = get_key("generickey")
    k3 = get_key("generickey")
    k4 = get_key("generickey")
    k5 = get_key("generickey")
    create_string_key(k1)
    create_list_key(k2)
    create_hash_key(k3)
    create_set_key(k4)
    create_zset_key(k5)

    cmd = f"del {k1} {k2}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def dump(name):
    k = get_key("generickey")
    cmd = f"dump {k}"

    create_string_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)

    create_list_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)

    create_zset_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)

    create_set_key(k)
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(k, "dump")

    create_hash_key(k)
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(k, "dump")


def exists(name: str):
    k1 = get_key("generickey")
    k2 = get_key("generickey")
    k3 = get_key("generickey")
    k4 = get_key("generickey")
    k5 = get_key("generickey")
    create_string_key(k1)
    create_list_key(k2)
    create_hash_key(k3)
    create_set_key(k4)
    create_zset_key(k5)
    k_random = get_key("generickey")

    cmd = f"exists {k1} {k2} {k3} {k4} {k5} {k_random}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def expire(name: str):
    k1 = get_key("generickey")
    k2 = get_key("generickey")
    k3 = get_key("generickey")
    k4 = get_key("generickey")
    k5 = get_key("generickey")
    create_string_key(k1)
    create_list_key(k2)
    create_hash_key(k3)
    create_set_key(k4)
    create_zset_key(k5)

    cmd = f"expire {k1} 1"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"expire {k2} 1"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"expire {k3} 1"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"expire {k4} 1"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"expire {k5} 1"
    r1.execute_command(cmd)
    r2.execute_command(cmd)

    time.sleep(2)

    cmd = f"exists {k1} {k2} {k3} {k4} {k5}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)

    for _ in range(0, random.randint(1, 100)):
        k_add_one = get_key("generickey")
        create_string_key(k_add_one)
    k_add_two = get_key("generickey")
    create_hash_key(k_add_two)


def expireat(name: str):
    k1 = get_key("generickey")
    k2 = get_key("generickey")
    k3 = get_key("generickey")
    k4 = get_key("generickey")
    k5 = get_key("generickey")
    create_string_key(k1)
    create_list_key(k2)
    create_hash_key(k3)
    create_set_key(k4)
    create_zset_key(k5)

    res = r1.execute_command("time")
    unix_time = res[0]

    cmd = f"expireat {k1} {unix_time+1}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"expireat {k2} {unix_time+1}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"expireat {k3} {unix_time+1}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"expireat {k4} {unix_time+1}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"expireat {k5} {unix_time+1}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)

    time.sleep(2)

    cmd = f"exists {k1} {k2} {k3} {k4} {k5}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)

    for _ in range(0, random.randint(1, 100)):
        k_add_one = get_key("generickey")
        create_string_key(k_add_one)
    k_add_two = get_key("generickey")
    create_hash_key(k_add_two)


def keys(name: str):
    pattern = "for_keys_pattern_"
    for _ in range(0, random.randint(1, 100)):
        k = get_key(pattern)
        create_string_key(k)
    cmd = f"keys {pattern}*"
    res1: list = r1.execute_command(cmd)
    res1.sort()
    res2: list = r2.execute_command(cmd)
    res2.sort()
    check(res1, res2, name, cmd)


def move(name: str):
    k1 = get_key("generickey")
    k2 = get_key("generickey")
    k3 = get_key("generickey")
    k4 = get_key("generickey")
    k5 = get_key("generickey")
    create_string_key(k1)
    create_list_key(k2)
    create_hash_key(k3)
    create_set_key(k4)
    create_zset_key(k5)

    cmd = f"move {k1} 1"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"move {k2} 2"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"move {k3} 3"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"move {k4} 4"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"move {k5} 5"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def rename(name: str):
    k1 = get_key("generickey")
    k2 = get_key("generickey")
    k3 = get_key("generickey")
    k4 = get_key("generickey")
    k5 = get_key("generickey")
    create_string_key(k1)
    create_list_key(k2)
    create_hash_key(k3)
    create_set_key(k4)
    create_zset_key(k5)

    k1_new = get_key("generickey")
    k2_new = get_key("generickey")
    k3_new = get_key("generickey")
    k4_new = get_key("generickey")
    k5_new = get_key("generickey")

    cmd = f"rename {k1} {k1_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"rename {k2} {k2_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"rename {k3} {k3_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"rename {k4} {k4_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"rename {k5} {k5_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def renamenx(name: str):
    k1 = get_key("generic_rename_key1_")
    k2 = get_key("generic_rename_key2_")
    k3 = get_key("generic_rename_key3_")
    k4 = get_key("generic_rename_key4_")
    k5 = get_key("generic_rename_key5_")
    create_string_key(k1)
    create_list_key(k2)
    create_hash_key(k3)
    create_set_key(k4)
    create_zset_key(k5)

    k1_new = get_key("generic_rename_new_key1_")
    k2_new = get_key("generic_rename_new_key2_")
    k3_new = get_key("generic_rename_new_key3_")
    k4_new = get_key("generic_rename_new_key4_")
    k5_new = get_key("generic_rename_new_key5_")

    cmd = f"renamenx {k1} {k1_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"renamenx {k2} {k2_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"renamenx {k3} {k3_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"renamenx {k4} {k4_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    cmd = f"renamenx {k5} {k5_new}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def restore(name: str):
    k1 = get_key("generickey")
    k2 = get_key("generickey")
    k3 = get_key("generickey")
    k4 = get_key("generickey")
    k5 = get_key("generickey")
    create_string_key(k1)
    create_list_key(k2)
    create_hash_key(k3)
    create_set_key(k4)
    create_zset_key(k5)

    k1_dump: str = r1.execute_command(f"dump {k1}")
    print(k1_dump)
    exit(1)

    k2_dump = r1.execute_command(f"dump {k2}")
    k3_dump = r1.execute_command(f"dump {k3}")
    k4_dump = r1.execute_command(f"dump {k4}")
    k5_dump = r1.execute_command(f"dump {k5}")

    k1_new = get_key("generickey")
    k2_new = get_key("generickey")
    k3_new = get_key("generickey")
    k4_new = get_key("generickey")
    k5_new = get_key("generickey")

    cmd = f"restore {k1_new} 0 \"{k1_dump}\" REPLACE"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(k1_new, "restore")
    cmd = f"restore {k2_new} 0 {k2_dump} REPLACE"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(k3_new, "restore")
    cmd = f"restore {k3_new} 0 {k3_dump} REPLACE"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(k3_new, "restore")
    cmd = f"restore {k4_new} 0 {k4_dump} REPLACE"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(k5_new, "restore")
    cmd = f"restore {k5_new} 0 {k5_dump} REPLACE"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    check_same(k5_new, "restore")


def sort_redis(name: str):
    k = get_key("generickey")
    create_list_key(k)
    d = get_key("generickey")
    if d == k:
        d = get_key("generickey")
    cmd = f"sort {k} LIMIT 0 5 ALPHA DESC STORE {d}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def type_redis(name: str):
    k = get_key("generickey")
    cmd = f"type {k}"
    create_string_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_list_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_hash_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_set_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_zset_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def unlink(name: str):
    k = get_key("generickey")
    cmd = f"unlink {k}"
    create_string_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_list_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_hash_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_set_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)
    create_zset_key(k)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def generic_cmd_table():
    cmds: dict = {#"copy": copy,
                  #"del_redis": del_redis,
                  #"dump": dump,
                  #"exists": exists,
                  #"expire": expire,
                  #"expireat": expireat,
                  #"keys": keys,
                  #"move": move,
                  #"rename": rename,
                  #"renamenx": renamenx,
                  #"restore": restore,      # resotre programing is not easy for dump and restore pair
                  #"sort_redis": sort_redis,
                  #"type_redis": type_redis,
                  "unlik": unlink,
                  }
    return cmds


def exec(name: str):
    val = get_val()
    other_val = get_val()

    str_key = get_key("tran_str_")
    create_string_key(str_key)
    str_cmds = (f"get {str_key}",
                f"set {str_key} {val}",
                f"substr {str_key} 0 5",
                f"getset {str_key} {val}",
                f"append {str_key} {other_val}",
                f"getdel {str_key}",
                f"setrange {str_key} 2 {other_val}",
                )

    list_key1 = get_key("tran_list_")
    create_list_key(list_key1)
    list_key2 = get_key("tran_list_")
    list_lset_key = get_key("tran_lset_")
    create_list_key(list_lset_key)
    list_cmds = (f"lindex {list_key1} 5",
                 f"linsert {list_key1} AFTER {val} {other_val}",
                 f"llen {list_key1}",
                 f"lmove {list_key1} {list_key2} RIGHT LEFT",
                 f"lpop {list_key1} 2",
                 f"lpos {list_key1} {other_val}",
                 f"lpush {list_key2} {other_val}",
                 f"lpushx {list_key1} {other_val}",
                 f"lrange {list_key1} 0 5",
                 f"lrem {list_key1} -1 {other_val}",
                 f"lset {list_lset_key} 0 {other_val}",
                 f"ltrim {list_key1} 3 5",
                 f"rpop {list_key1} 2",
                 f"rpoplpush {list_key1} {list_key2}",
                 f"rpush {list_key2} {other_val}",
                 f"lpushx {list_key1} {other_val}",
                 )

    bitmap_key1 = get_key("tran_bm_src1_")
    create_string_key(bitmap_key1)
    bitmap_key2 = get_key("tran_bm_src2_")
    create_string_key(bitmap_key2)
    dest_bm_kkey = get_key("tran_bm_dest_")
    bitmap_cmds = (f"bitcount {bitmap_key1} 2 -2",
                   f"bitfield {bitmap_key1} INCRBY i5 100 1 GET u4 0",
                   f"bitop AND {dest_bm_kkey} {bitmap_key1} {bitmap_key2}",
                   f"bitpos {bitmap_key1} 0 2 -2",
                   f"getbit {bitmap_key1} 5",
                   f"setbit {bitmap_key1} 7 0",
                   )

    hash_key = get_key("tran_hash_")
    fields = create_hash_key(hash_key)
    hash_f1 = random.choice(fields)
    val1 = get_val()
    hash_f2 = random.choice(fields)
    val2 = get_val()
    hash_f3 = random.choice(fields)
    hash_cmds = (f"hdel {hash_key} {hash_f1} {hash_f2}",
                 f"hexists {hash_key} {hash_f3}",
                 f"hget {hash_key} {hash_f3}",
                 f"hlen {hash_key}",
                 f"hset {hash_key} {hash_f1} {val1} {hash_f2} {val2}",
                 f"hstrlen {hash_key} {hash_f3}",
                 #f"hvals {hash_key}",
                 )

    set_key = get_key("tran_set_")
    members = create_set_key(set_key)
    exist_m = random.choice(members)
    random_m = get_key("m_")
    set_add_m1 = get_key("m_")
    set_add_m2 = get_key("m_")
    set_cmds = (f"sadd {set_key} {set_add_m1} {set_add_m2}",
                f"scard {set_key}",
                f"sismember {set_key} {exist_m}",
                f"sismember {set_key} {random_m}",
                f"srem {set_key} {exist_m} {random_m}",
                )

    zset_key = get_key("tran_zset_")
    ms: dict = create_zset_key(zset_key)
    m1 = random.choice(list(ms.keys()))
    m2 = random.choice(list(ms.keys()))
    new_m = get_key("m")
    new_s = random.randint(-1000_000, 1000_000)
    zset_cmds = (f"zadd {zset_key} NX {new_s} {new_m}",
                 f"zcard {zset_key}",
                 f"zcount {zset_key} -1000 1000",
                 f"zincrby {zset_key} 10 {m1}",
                 f"zlexcount {zset_key} [b [f",
                 f"zpopmax {zset_key} 2",
                 f"zpopmin {zset_key} 2",
                 f"zrange {zset_key} -10000 10000",
                 f"zrangebylex {zset_key} [aaa (g",
                 f"zrangebyscore {zset_key} -100 100",
                 f"zremrangebyrank {zset_key} 2 4",
                 f"zremrangebylex {zset_key} [alpha [omega",
                 f"zremrangebyscore {zset_key} -10 10",
                 f"zrank {zset_key} {m2}",
                 f"zrevrank {zset_key} {m2}",
                 f"zscore {zset_key} {m2}",
                 )

    cmd_types = ("string", "list", "bitmap", "hash", "set", "zset")

    pipe1 = r1.pipeline(transaction=True)
    pipe2 = r2.pipeline(transaction=True)

    for _ in range(2, 20):
        cmd_type = random.choice(cmd_types)
        if cmd_type == "string":
            cmd = random.choice(str_cmds)
        elif cmd_type == "list":
            cmd = random.choice(list_cmds)
        elif cmd_type == "bitmap":
            cmd = random.choice(bitmap_cmds)
        elif cmd_type == "hash":
            cmd = random.choice(hash_cmds)
        elif cmd_type == "set":
            cmd = random.choice(set_cmds)
        elif cmd_type == "zset":
            cmd = random.choice(zset_cmds)
        else:
            raise Exception(f"unrecognized cmd_type = {cmd_type}")
        pipe1.execute_command(cmd)
        pipe2.execute_command(cmd)

    res1 = pipe1.execute()
    res2 = pipe2.execute()
    check(res1, res2, name, "transaction command")


def discard(name: str):
    str_key = get_key("tran_str_")

    pipe1 = r1.pipeline(transaction=True)
    pipe2 = r2.pipeline(transaction=True)

    cmd = f"set {str_key} val"
    pipe1.execute_command(cmd)
    pipe2.execute_command(cmd)
    cmd = f"getset {str_key} new_val"
    pipe1.execute_command(cmd)
    pipe2.execute_command(cmd)
    cmd = "discard"
    pipe1.execute_command(cmd)
    pipe2.execute_command(cmd)


def thread_incr_key(k: str):
    cmd = f"incr {k}"
    r1_thread.execute_command(cmd)
    r2_thread.execute_command(cmd)
    #print("thread_incr_key finish!")


def watch(name: str):
    k = get_key("tran_watch_")
    v = random.randint(-100, 100)
    k_v = v + 1000_000

    #print(f"k = {k}, v = {v}, k_v = {k_v}")
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"watch {k}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)

    # another thread modify the key
    t = threading.Thread(target=thread_incr_key, args=(k,))
    t.start()

    cmd = "multi"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"set {k} {k_v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    time.sleep(0.1)
    cmd = "exec"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"get {k}"
    check1 = r1.execute_command(cmd)
    check2 = r2.execute_command(cmd)
    if check1 != check2:
        raise Exception(f"watch, key = {k}, v = {v}, k_v = {k_v}")
    #print(f"check1 = {check1}, check2 = {check2}")
    #exit(1)


def transaction_cmd_table():
    cmds: dict = {"exec": exec,
                  "discard": discard,
                  "watch": watch,
                  }
    return cmds


def eval(name: str):
    s = "return ARGV[1]"
    res1 = r1.eval(s, 0, "hello")
    res2 = r2.eval(s, 0, "hello")
    check(res1, res2, name, "eval")
    s = "return redis.call('SET', KEYS[1], ARGV[1])"
    k = get_key("lua_eval_")
    v = get_val()
    res1 = r1.eval(s, 1, k, v)
    res2 = r2.eval(s, 1, k, v)
    check(res1, res2, name, "eval set")
    check_k_v = r1.execute_command(f"get {k}")
    if check_k_v != v:
        raise Exception(f"eval failed, key = {k}")
    r1.execute_command(f"rockevict {k}")
    append_str = get_val()
    s = "return redis.call('APPEND', KEYS[1], ARGV[1])"
    res1 = r1.eval(s, 1, k, append_str)
    res2 = r2.eval(s, 1, k, append_str)
    check(res1, res2, name, "eval append")


def script_load_and_sha(name: str):
    s = "return ARGV[1]"
    sha1 = r1.script_load(s)
    sha2 = r2.script_load(s)
    if sha1 != sha2:
        raise Exception("script_load_and_sha, sha1 != sha2")
    arg_v1 = "hello"
    res1 = r1.evalsha(sha1, 0, arg_v1)
    if res1 != arg_v1:
        raise Exception("script_load_and_sha, res1 != arg_v1")
    arg_v2 = "world"
    res2 = r2.evalsha(sha2, 0, arg_v2)
    if res2 != arg_v2:
        raise Exception("script_load_and_sha, res2 != arg_v2")



def lua_cmd_table():
    cmds: dict = {"eval": eval,
                  "script_load_and_sha": script_load_and_sha,
                  }
    return cmds

def init_cmd_table(table: str):
    if table == "str":
        return string_cmd_table()
    elif table == "list":
        return list_cmd_table()
    elif table == "bitcount":
        return bitmap_cmd_table()
    elif table == "hash":
        return hash_cmd_table()
    elif table == "set":
        return set_cmd_table()
    elif table == "zset":
        return zset_cmd_table()
    elif table == "generic":
        return generic_cmd_table()
    elif table == "transaction":
        return transaction_cmd_table()
    elif table == "lua":
        return lua_cmd_table()
    elif table == "all":
        str_cmds = string_cmd_table()
        list_cmds = list_cmd_table()
        return {**str_cmds, **list_cmds}
    else:
        print("un-recognize table, select one from all, str, list")
        return {}


def _main():
    global r1, r2, r1_thread, r2_thread
    r1, r2, r1_thread, r2_thread = init_redis_clients()
    cmd_table = sys.argv[1]

    max_cmd_num = sys.maxsize
    if len(sys.argv) >= 3:
        max_cmd_num = int(sys.argv[2])

    if cmd_table == "flushall":
        r1.execute_command("flushall")
        r2.execute_command("flushall")
        print("flush all for redrock and real redis")
    elif cmd_table == "inject":
        init_redrock(r1)
        print("inject finished!")
    else:
        init_redrock(r1)
        cmds:list = list(init_cmd_table(cmd_table).items())
        if not cmds:
            exit(1)
        cnt = 0

        while True:
            dice = random.choice(cmds)
            cmd_name: str = dice[0]
            cmd_func: callable = dice[1]
            if cmd_name == "setex":
                # sleep in setex(), so dice2
                if random.randint(0, 1) == 0:
                    cmd_func(cmd_name)
                    cnt = cnt + 1
            else:
                cmd_func(cmd_name)
                cnt = cnt + 1

            if cnt % 1000 == 0:
                print(f"cnt = {cnt}, time = {time.time()}")

            if cnt >= max_cmd_num:
                print(f"finish by max limit of cnt = {cnt}")
                break


if __name__ == '__main__':
    _main()