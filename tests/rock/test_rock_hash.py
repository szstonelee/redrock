from conn import r, rock_evict_hash


key = "_test_rock_hash_"


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
    #rock_evict_hash(key, "f1", "f2")
    #res = r.execute_command("hdel", key, "f2")
    #if res != 1:
    #    print(res)
    #    raise Exception("rock_hash: hdel 2")


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
    rock_evict_hash(key, "f1", "f3")
    res = r.execute_command("hget", key, "f1")
    if res != "v1":
        print(res)
        raise Exception("rock_hash: hget")


def hgetall():
    build_rock_hash(key)
    rock_evict_hash(key, "f1", "f3")
    res = r.execute_command("hgetall", key)
    if res != {"f1": "v1", "f2": "v2", "f3": "v3", "f4": "v4", "f5": "v5", "f6": "v6"}:
        print(res)
        raise Exception("rock_hash: hgetall")


def test_all():
    hdel()
    #hexists()
    #hget()
    #hgetall()


def _main():
    while (1):
        test_all()


if __name__ == '__main__':
    _main()