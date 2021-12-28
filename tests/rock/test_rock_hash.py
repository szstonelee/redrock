from conn import r, rock_evict_hash
import redis


key = "_test_rock_hash2_"


# assume a hash more than 4 fields will be in a rock hash
def build_rock_hash(k):
    r.execute_command("del", k)
    r.execute_command("hset", k, "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5", "f6", "v6")


def hdel():
    build_rock_hash(key)
    rock_evict_hash(key, "f2", "f2")
    res = r.execute_command("hdel", key, "f1")
    if res != 1:
        print(res)
        raise Exception("rock_hash: hdel")
    rock_evict_hash(key, "f1", "f2")
    res = r.execute_command("hdel", key, "f2")
    if res != 1:
        print(res)
        raise Exception("rock_hash: hdel 2")


def hexists():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f3")
    res = r.execute_command("hexists", key, "f1")
    if res != 1:
         print(res)
         raise Exception("rock_hash: hexists")
    res = r.execute_command("hexists", key, "f2")
    if res != 1:
         print(res)
         raise Exception("rock_hash: hexists 2")
    res = r.execute_command("hexists", key, "f3")
    if res != 1:
         print(res)
         raise Exception("rock_hash: hexists 3")


def hget():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f3", "f1", "f3")
    res = r.execute_command("hget", key, "f1")
    if res != "v1":
        print(res)
        raise Exception("rock_hash: hget")
    res = r.execute_command("hget", key, "f2")
    if res != "v2":
        print(res)
        raise Exception("rock_hash: hget2")


def hgetall():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f3")
    res = r.execute_command("hgetall", key)
    if res != {"f1": "v1", "f2": "v2", "f3": "v3", "f4": "v4", "f5": "v5", "f6": "v6"}:
        print(res)
        raise Exception("rock_hash: hgetall")


def hincrby():
    build_rock_hash(key)
    r.execute_command("hset", key, "f1", "123")
    rock_evict_hash(key, "f1", "f2")
    res = r.execute_command("hincrby", key, "f1", 6)
    if res != 129:
        print(res)
        raise Exception("rock_hash: hincrby")
    try:
        r.execute_command("hincrby", key, "f2", 1)
    except redis.exceptions.ResponseError as e:
        if (str(e) != "hash value is not an integer"):
            print(e)
            raise Exception("rock_hash: hincrby 2")


def hincrbyfloat():
    build_rock_hash(key)
    r.execute_command("hset", key, "f1", "10.5")
    rock_evict_hash(key, "f1", "f2")
    res = r.execute_command("hincrbyfloat", key, "f1", 0.1)
    if res != 10.6:
        print(res)
        raise Exception("rock_hash: hincrbyfloat")

def hkeys():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f2")
    res: list = r.execute_command("hkeys", key)
    res.sort()
    if res != ["f1", "f2", "f3", "f4", "f5", "f6"]:
        print(res)
        raise Exception("rock_hash: hkeys")


def hlen():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f2")
    res = r.execute_command("hlen", key)
    if res != 6:
        print(res)
        raise Exception("rock_hash: hlen")


def hmget():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f2")
    res = r.execute_command("hmget", key, "f1", "f3", "f1", "f3", "f_not_exist", "f_not_exist")
    if res != ["v1", "v3", "v1", "v3", None, None]:
        print(res)
        raise Exception("rock_hash: hmget")


def hmset():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f2")
    r.execute_command("hmset", key, "f1", "new_v1", "f3", "new_v3", "f100", "v100")
    res = r.execute_command("hmget", key, "f1", "f2", "f3", "f100")
    if res != ["new_v1", "v2", "new_v3", "v100"]:
        print(res)
        raise Exception("rock_hash: hmset")


def hset():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f2")
    r.execute_command("hset", key, "f1", "new_v1", "f3", "new_v3", "f100", "v100")
    res = r.execute_command("hmget", key, "f1", "f2", "f3", "f100")
    if res != ["new_v1", "v2", "new_v3", "v100"]:
        print(res)
        raise Exception("rock_hash: hset")


def hsetnx():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f2")
    res = r.execute_command("hsetnx", key, "f1", "new_v1")
    if res != 0:
        print(res)
        raise Exception("rock_hash: hsetnx")
    res = r.execute_command("hsetnx", key, "fnew", "fnew_val")
    if res != 1:
        print(res)
        raise Exception("rock_hash: hsetnx 2")
    res = r.execute_command("hmget", key, "f1", "f2", "fnew")
    if res != ["v1", "v2", "fnew_val"]:
        print(res)
        raise Exception("rock_hash: hsetnx 3")


def hrandfield():
    build_rock_hash(key)
    cmp = {"f1":"v1", "f2":"v2", "f3":"v3", "f4":"v4", "f5":"v5", "f6":"v6"}
    rock_evict_hash(key, "f1", "f2", "f3", "f4")
    res: list = r.execute_command("hrandfield", key, 2, "withvalues")
    item_len = len(res)
    if item_len % 2 != 0:
        print(res)
        raise Exception("rock_hash: hrandfield")
    for i in range(0, int(item_len/2)):
        k = res[i*2]
        v = res[i*2+1]
        if k not in cmp:
            print(res)
            raise Exception("rock_hash: hrandfield 2")
        if v != cmp[k]:
            print(res)
            raise Exception("rock_hash: hrandfield 3")


def hstrlen():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f2", "f3", "f4")
    res = r.execute_command("hstrlen", key, "f1")
    if res != 2:
        print(res)
        raise Exception("rock_hash: hstrlen")


def hvals():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f2", "f3", "f4")
    res: list = r.execute_command("hvals", key)
    res.sort()
    if res != ["v1", "v2", "v3", "v4", "v5", "v6"]:
        print(res)
        raise Exception("rock_hash: hvals")


def test_all():
    hdel()
    hexists()
    hget()
    hgetall()
    hincrby()
    hincrbyfloat()
    hkeys()
    hlen()
    hmget()
    hmset()
    hset()
    hsetnx()
    hrandfield()
    hstrlen()
    hvals()


def _main():
    cnt = 0
    while (1):
        test_all()
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test rock hash OK cnt = {cnt}")


if __name__ == '__main__':
    _main()