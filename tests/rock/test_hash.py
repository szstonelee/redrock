from conn import r, rock_evict


key = "_test_rock_hash_"


def hdel():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "foo", "field2", "bar")
    rock_evict(key)
    res = r.hdel(key, "field1")
    if res != 1:
        print(res)
        raise Exception("hdel fail")
    rock_evict(key)
    res = r.hdel(key, "field3")
    if res != 0:
        print(res)
        raise Exception("hdel fail2")
    r.execute_command("hset", key, "field4", "val4")
    rock_evict(key)
    res = r.execute_command("del", key, "field4", "field3", "field4")
    if res != 1:
        print(res)
        raise Exception("hdel fail3")


def hexists():
    r.execute_command("del", key)
    r.hset(key, "field1", "foo")
    rock_evict(key)
    #res = r.hexists(key, "field1")
    #if res != 1:
    #     print(res)
    #     raise Exception("hexists fail")
    # rock_evict(key)
    # res = r.hexists(key, "field2")
    # if res != 0:
    #     print(res)
    #     raise Exception("hexists fail2")


def hget():
    r.execute_command("del", key)
    r.hset(key, "field1", "foo")
    rock_evict(key)
    res = r.hget(key, "field1")
    if res != "foo":
        print(res)
        raise Exception("hget fail")
    rock_evict(key)
    res = r.hget(key, "field2")
    if res is not None:
        print(res)
        raise Exception("hget fail2")


def hgetall():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "foo", "field2", "bar")
    rock_evict(key)
    res = r.hgetall(key)
    if res != {"field1": "foo", "field2": "bar"}:
        print(res)
        raise Exception("hgetall fail")


def hincrby():
    r.execute_command("del", key)
    r.hset(key, "field1", 5)
    rock_evict(key)
    res = r.hincrby(key, "field1", 1)
    if res != 6:
        print(res)
        raise Exception("hicrby fail")


def hincrbyfloat():
    r.execute_command("del", key)
    r.hset(key, "field", 10.5)
    rock_evict(key)
    res = r.hincrbyfloat(key, "field", 0.1)
    if res != 10.6:
        print(res)
        raise Exception("hincrbyfloat fail")


def hkeys():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "foo", "field2", "bar")
    rock_evict(key)
    res = r.hkeys(key)
    if res != ["field1", "field2"]:
        print(res)
        raise Exception("hkeys fail")


def hlen():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "foo", "field2", "bar")
    #rock_evict(key)
    #res = r.hlen(key)
    #if res != 2:
    #    print(res)
    #    raise Exception("hlen fail")


def hmget():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "foo", "field2", "bar")
    rock_evict(key)
    res = r.hmget(key, "field1", "field2", "nofiled")
    if res != ["foo", "bar", None]:
        print(res)
        raise Exception("hmget fail")
    rock_evict(key)
    res = r.execute_command("hmget", key, "field1", "field2", "nofiled", "field1", "field2", "nofiled")
    if res != ["foo", "bar", None, "foo", "bar", None]:
        print(res)
        raise Exception("hmget fail2")


def hmset():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "foo", "field2", "bar")
    rock_evict(key)
    r.execute_command("hmset", key, "field1", "new_foo", "field3", "val3")
    res = r.hgetall(key)
    if res != {"field1": "new_foo", "field2": "bar", "field3": "val3"}:
        print(res)
        raise Exception("hmset fail")
    rock_evict(key)
    r.execute_command("hmset", key, "field1", "new_foo", "field2", "bar", "field1", "back_to_foo")
    res = r.hgetall(key)
    if res != {'field2': 'bar', 'field1': 'back_to_foo', 'field3': 'val3'}:
        print(res)
        raise Exception("hmset fail2")


def hset():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "foo", "field2", "bar")
    rock_evict(key)
    r.execute_command("hset", key, "field1", "new_foo", "field3", "val3")
    res = r.hgetall(key)
    if res != {"field1": "new_foo", "field2": "bar", "field3": "val3"}:
        print(res)
        raise Exception("hset fail")


def hset_for_ht_encoding():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "foo", "field2", "bar")
    long_val = "val" * 1000
    rock_evict(key)
    r.execute_command("hset", key, "field1", long_val, "field3", "val3")
    res = r.hgetall(key)
    if res != {"field1": long_val, "field2": "bar", "field3": "val3"}:
        print(res)
        raise Exception("hset_for_ht_encoding fail")


def hsetnx():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field", "Hello")
    rock_evict(key)
    res = r.execute_command("hsetnx", key, "field", "World")
    if res != 0:
        print(res)
        raise Exception("hsetnx fail")
    rock_evict(key)
    r.hsetnx(key, "other_field", "World")
    res = r.hgetall(key)
    if res != {"field": "Hello", "other_field": "World"}:
        print(res)
        raise Exception("hsetnx fail2")


def hrandfield():
    r.execute_command("del", key)
    r.execute_command("hset", key, "heads", "obverse", "tails", "reverse", "edge", "null")
    all = r.hgetall(key)
    rock_evict(key)
    res = r.execute_command("hrandfield", key, 2, "withvalues")
    if len(res) != 4:
        print(res)
        raise Exception("hrandfield fail")
    for i in range(0, 2):
        f = res[i*2]
        v = res[i*2+1]
        if not (f in all and v == all[f]):
            print(res)
            raise Exception("hrandfield fail2")


def hstrlen():
    r.execute_command("del", key)
    r.execute_command("hset", key, "heads", "obverse", "tails", "reverse", "edge", "null")
    rock_evict(key)
    res = r.hstrlen(key, "tails")
    if res != 7:
        print(res)
        raise Exception("hstrlen fail")


def hvals():
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "hello", "field2", "world")
    rock_evict(key)
    res: list = r.hvals(key)
    res.sort()
    if res != ["hello", "world"]:
        print(res)
        raise Exception("hvals fail")
    

def test_all():
    #hdel()
    #hexists()
    # hget()
    # hgetall()
    # hincrby()
    # hincrbyfloat()
    # hkeys()
    hlen()
    # hmget()
    # hmset()
    # hset()
    # hset_for_ht_encoding()
    # hsetnx()
    # hrandfield()
    # hstrlen()
    # hvals()


def _main():
    cnt = 0
    while (1):
        test_all()
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test str OK cnt = {cnt}")


if __name__ == '__main__':
    _main()