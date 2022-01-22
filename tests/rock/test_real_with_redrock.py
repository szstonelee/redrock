#./redrock --port 7000 --cluster-enabled yes --cluster-config-file node7000.conf --cluster-node-timeout 5000 --save 0

#https://www.snel.com/support/how-to-install-grafana-graphite-and-statsd-on-ubuntu-18-04/

import redis
import random
import string
import time


r1: redis.StrictRedis   # redrock
r2: redis.StrictRedis   # real redis 6.2.2


def init_redis_clients():
    r1_ip = "192.168.64.4"
    r2_ip = r1_ip
    r1_port = 6379
    r2_port = 6380
    pool1 = redis.ConnectionPool(host=r1_ip,
                                 port=r1_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='utf-8',
                                 socket_connect_timeout=2)
    pool2 = redis.ConnectionPool(host=r2_ip,
                                 port=r2_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='utf-8',
                                 socket_connect_timeout=2)
    r1: redis.StrictRedis = redis.StrictRedis(connection_pool=pool1)
    r2: redis.StrictRedis = redis.StrictRedis(connection_pool=pool2)
    return r1, r2


def init_redrock(r: redis.StrictRedis):
    r.execute_command("config set hash-max-ziplist-entries 2")
    r.execute_command("config set hash-max-rock-entries 4")
    r.execute_command("config set maxrockmem 20000000")
    #r.execute_command("config set save '3600 1 300 100 60 10000'")
    r.execute_command("config set appendonly yes")


def get_str_key():
    key = "strkey_"
    for _ in range(0, random.randint(1, 100)):
        key = key + random.choice(string.digits)
    return key


def get_str_keys():
    keys = []
    for _ in range(0, random.randint(1, 10)):
        key = "strkey_"
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


def set(name: str):
    k = get_str_key()
    v = get_val()
    cmd = f"set {k} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def strlen(name: str):
    k = get_str_key()
    cmd = f"strlen {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def append(name: str):
    k = get_str_key()
    exist = r2.execute_command(f"exists {k}")
    if not exist:
        return
    v = get_val()
    cmd = f"append {k} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def decr(name: str):
    k = get_str_key()
    v = get_int()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"decr {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def incr(name: str):
    k = get_str_key()
    v = get_int()
    cmd = f"set {k} {v}"
    r1.execute_command(cmd)
    r2.execute_command(cmd)
    cmd = f"incr {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def decrby(name: str):
    k = get_str_key()
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
    k = get_str_key()
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
    k = get_str_key()
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
    k = get_str_key()
    cmd = f"get {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def getdel(name: str):
    k = get_str_key()
    cmd = f"getdel {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def getex(name: str):
    k = get_str_key()
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
    k = get_str_key()
    start = random.randint(0, 10)
    end = start + random.randint(0, 1000)
    cmd = f"getrange {k} {start} {end}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def getset(name: str):
    k = get_str_key()
    v = get_val()
    cmd = f"getset {k} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def mget(name: str):
    ks = get_str_keys()
    cmd = f"mget "
    for k in ks:
        cmd = cmd + " " + k
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def mset(name: str):
    ks = get_str_keys()
    cmd = f"mset "
    for k in ks:
        cmd = cmd + " " + k
        val = get_val()
        cmd = cmd + " " + val
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def msetnx(name: str):
    ks = get_str_keys()
    cmd = f"msetnx "
    for k in ks:
        cmd = cmd + " " + k
        val = get_val()
        cmd = cmd + " " + val
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def psetex(name: str):
    k = get_str_key()
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
    k = get_str_key()
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
    k = get_str_key()
    v = get_val()
    cmd = f"setnx {k} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def setrange(name: str):
    k = get_str_key()
    offset = random.randint(1, 100)
    v = get_val()
    cmd = f"setrange {k} {offset} {v}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def substr(name: str):
    k = get_str_key()
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


def get_list_key():
    key = "listkey_"
    for _ in range(0, random.randint(1, 100)):
        key = key + random.choice(string.digits)
    return key


def lindex(name: str):
    k = get_list_key()
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
    k = get_list_key()
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
    k = get_list_key()
    cmd = f"llen {k}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lmove(name: str):
    src = get_list_key()
    dst = get_list_key()
    cmd = f"lmove {src} {dst} right left"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lpop(name: str):
    k = get_list_key()
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
    k = get_list_key()
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
    k = get_list_key()
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
    k = get_list_key()
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = f"lpush {k} {v}"
        res1 = r1.execute_command(cmd)
        res2 = r2.execute_command(cmd)
        check(res1, res2, name, cmd)


def rpush(name: str):
    k = get_list_key()
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = f"rpush {k} {v}"
        res1 = r1.execute_command(cmd)
        res2 = r2.execute_command(cmd)
        check(res1, res2, name, cmd)


def lpushx(name: str):
    k = get_list_key()
    cmd = f"lpushx {k}"
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = cmd + " " + str(v)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def rpushx(name: str):
    k = get_list_key()
    cmd = f"rpushx {k}"
    for _ in range(0, random.randint(1, 1000)):
        v = random.randint(0, 999999)
        cmd = cmd + " " + str(v)
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lrange(name: str):
    k = get_list_key()
    start = random.randint(0, 10)
    end = start + random.randint(5, 100)
    cmd = f"lrange {k} {start} {end}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lrem(name: str):
    k = get_list_key()
    element = random.randint(1, 999)
    cmd = f"lrem {k} -2 {element}"
    res1 = r1.execute_command(cmd)
    res2 = r2.execute_command(cmd)
    check(res1, res2, name, cmd)


def lset(name: str):
    k = get_list_key()
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
    k = get_list_key()
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
    src = get_list_key()
    dst = get_list_key()
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



def init_cmd_table():
    str_cmds: dict = string_cmd_table()
    list_cmds: dict = list_cmd_table()
    return {**str_cmds, **list_cmds}
    #return list_cmds


def _main():
    global r1, r2
    r1, r2 = init_redis_clients()
    r1.execute_command("flushall")
    r2.execute_command("flushall")
    init_redrock(r1)
    cmds:list = list(init_cmd_table().items())
    cnt = 0

    while True:
        dice = random.choice(cmds)
        cmd_name: str = dice[0]
        cmd_func: callable = dice[1]
        if cmd_name == "setex":
            # sleep in setex(), so dice2
            if random.randint(0, 100) == 0:
                cmd_func(cmd_name)
                cnt = cnt + 1
        else:
            cmd_func(cmd_name)
            cnt = cnt + 1

        if cnt % 1000 == 0:
            print(f"cnt = {cnt}, time = {time.time()}")


if __name__ == '__main__':
    _main()