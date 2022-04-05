#./redrock --port 7000 --cluster-enabled yes --cluster-config-file node7000.conf --cluster-node-timeout 5000 --save 0

#https://www.snel.com/support/how-to-install-grafana-graphite-and-statsd-on-ubuntu-18-04/

import redis
import random
import string
import time
import sys


r1: redis.StrictRedis   # redrock
r2: redis.StrictRedis   # real redis 6.2.2


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
    return r1, r2


def insert_50K_keys_for_redrock():
    print("starting insert 50k keys to RedRock so RedRock will use disk for this test...")
    for i in range(0, 25_000):
        if i % 1000 == 0:
            print(f"insert_50K_keys_for_redrock(), i = {i}")
        # string first
        k = "init_for_redrock_hash_" + str(i)
        field_v = "fv" * 500
        cmd = f"hmset {k} f1 {field_v} f2 {field_v} f3 {field_v} f4 {field_v} f5 {field_v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
        # then hash
        k = "init_for_redrcok_str_" + str(i)
        v = "v" * 1000
        cmd = f"set {k} {v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)

    print("insert_50K_keys_for_redrock finished!!!!")


def init_redrock(r: redis.StrictRedis):
    r.execute_command("config set hash-max-ziplist-entries 2")
    r.execute_command("config set hash-max-rock-entries 4")
    r.execute_command("config set maxrockmem 50000000")  # 50M
    r.execute_command("config set appendonly yes")
    dbsize = r.execute_command("dbsize")
    print(f"dbsize = {dbsize}")
    if dbsize < 40_000:
        insert_50K_keys_for_redrock()
    #r.execute_command("config set save '3600 1 300 100 60 10000'")


def get_key(key_prefix: str):
    key = key_prefix
    for _ in range(0, random.randint(1, 100)):
        key = key + random.choice(string.digits)
    return key


def get_fields(field_prefix: str):
    fields = []
    for _ in range(0, 100):
        fields.append(field_prefix + str(random.randint(1, 1000)))
    return fields


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
    cmd = f"dump {key}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    if res1 != res2:
        msg = f"check_compare fail for {key} dump. caller = {caller}"
        print(msg)
        raise Exception(msg)


def set(name: str):
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
    cmds: dict = {"set": set,
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


def hdel(name: str):
    k = get_key("hashkey")
    fs = get_fields("f")
    for f in fs:
        v = get_val()
        cmd = f"hset {k} {f} {v}"
        r1.execute_command(cmd)
        r2.execute_command(cmd)
    f1 = random.choice(fs)
    f2 = random.choice(fs)
    f3 = random.choice(fs)
    cmd = f"hdel {k} {f1} {f2} {f3}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def hash_cmd_table():
    cmds: dict = {"hdel": hdel,
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
    elif table == "all":
        str_cmds = string_cmd_table()
        list_cmds = list_cmd_table()
        return {**str_cmds, **list_cmds}
    else:
        print("un-recognize table, select one from all, str, list")
        return {}


def _main():
    global r1, r2
    r1, r2 = init_redis_clients()
    cmd_table = sys.argv[1]

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


if __name__ == '__main__':
    _main()