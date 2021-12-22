import redis
from conn import r, rock_evict


key = "_test_rock_multi_"


def empty_tran():
    pipe = r.pipeline(transaction=True)
    res = pipe.execute()
    if res:
        print(res)
        raise Exception("multi: empty_tran")


def simple():
    k1 = key + "_k1"
    k2 = key + "_k2"
    r.execute_command("set", k1, "abc")
    r.execute_command("set", k2, "defg")
    rock_evict(k1, k2)
    pipe = r.pipeline(transaction=True)
    pipe.execute_command("get", k1)
    pipe.execute_command("get", k2)
    res = pipe.execute()
    if res != ["abc", "defg"]:
        print(res)
        raise Exception("multi: simple")


def simple_with_error():
    k1 = key + "_k1"
    k2 = key + "_k2"
    r.execute_command("set", k1, "abc")
    r.execute_command("set", k2, "defg")
    rock_evict(k1, k2)
    try:
        pipe = r.pipeline(transaction=True)
        pipe.execute_command("get", k1)
        pipe.execute_command("get", k1, k2)
        res = pipe.execute()
        print(res)
    except redis.exceptions.ResponseError:
        pass


def error_but_part_success():
    r.execute_command("set", key, "")
    rock_evict(key)
    try:
        pipe = r.pipeline(transaction=True)
        pipe.execute_command("set", key, "i am not integer")
        pipe.execute_command("incr", key)
        res = pipe.execute()
        print(res)
    except redis.exceptions.ResponseError:
        pass
    res = r.execute_command("get", key)
    if res != "i am not integer":
        print(res)
        raise Exception("multi: error_but_part_success")


def evict_in_half():
    r.execute_command("set", key, "")
    rock_evict(key)
    pipe = r.pipeline(transaction=True)
    pipe.execute_command("set", key, "abc")
    r.execute_command("rockevict", key)     # NOTE: pipe.rockevict is wrong!!!
    pipe.execute_command("append", key, "123")
    pipe.execute()
    res = r.execute_command("get", key)
    if res != "abc123":
        print(res)
        raise Exception("multi: evict_in_half")


def move_in_multi():
    r.execute_command("select", 0)
    r.execute_command("set", key, "abc")
    r.execute_command("select", 1)
    r.execute_command("del", key)
    r.execute_command("select", 0)
    rock_evict(key)
    pipe = r.pipeline(transaction=True)
    pipe.execute_command("move", key, 1)
    pipe.execute_command("select", 1)
    pipe.execute_command("append", key, "123")
    pipe.execute()
    res = r.execute_command("get", key)
    if res != "abc123":
        print(res)
        raise Exception("multi: move_in_multi")
    r.execute_command("select", 0)


def blpop_in_multi():
    r.execute_command("del", key)
    r.execute_command("rpush", key, 1)
    r.execute_command("lpop", key)
    rock_evict(key)
    pipe = r.pipeline(transaction=True)
    pipe.execute_command("blpop", key, 0)
    res = pipe.execute()
    if res != [None]:
        print(res)
        raise Exception("multi: blpop_in_multi")


def discard():
    r.execute_command("set", key, 1)
    rock_evict(key)
    r.execute_command("multi")
    r.execute_command("incr", key)
    r.execute_command("discard")
    res = r.execute_command("get", key)
    if res != "1":
        print(res)
        raise Exception("multi: discard")


def watch_success():
    r.execute_command("set", key, 1)
    r.execute_command("watch", key)
    rock_evict(key)
    pipe = r.pipeline(transaction=True)
    pipe.execute_command("incr", key)
    pipe.execute()
    res = r.execute_command("get", key)
    if res != "2":
        print(res)
        raise Exception("multi: watch_success")


def watch_fail():
    r.execute_command("set", key, 1)
    r.execute_command("watch", key)
    rock_evict(key)
    r.execute_command("set", key, 10)
    rock_evict(key)
    try:
        pipe = r.pipeline(transaction=True)
        pipe.execute_command("incr", key)
        pipe.execute()
    except redis.exceptions.WatchError:
        pass
    res = r.execute_command("get", key)
    if res != "10":
        print(res)
        raise Exception("multi: watch_fail")


def unwatch():
    r.execute_command("set", key, 1)
    r.execute_command("watch", key)
    rock_evict(key)
    r.execute_command("set", key, 10)
    rock_evict(key)
    r.execute_command("unwatch")
    pipe = r.pipeline(transaction=True)
    pipe.execute_command("incr", key)
    pipe.execute()
    res = r.execute_command("get", key)
    if res != "11":
        print(res)
        raise Exception("multi: unwatch")


def mix():
    k1 = key + "_k1"
    k2 = key + "_k2"
    k3 = key + "_k3"
    k4 = key + "_k4"
    r.execute_command("set", k1, "abc")
    r.execute_command("del", k2)
    r.execute_command("rpush", k2, 1)
    r.execute_command("del", k3)
    r.execute_command("sadd", k3, "a", "b", "c")
    r.execute_command("del", k4)
    r.execute_command("zadd", k4, 1, "one", 2, "two", 3, "three")
    rock_evict(k1, k2, k3, k4)
    pipe = r.pipeline(transaction=True)
    pipe.execute_command("append", k1, "123")
    pipe.execute_command("rpush", k2, 2)
    pipe.execute_command("srem", k3, "b")
    pipe.execute_command("zpopmax", k4)
    pipe.execute()
    res = r.execute_command("get", k1)
    if res != "abc123":
        print(res)
        raise Exception("multi: mix")
    res = r.execute_command("llen", k2)
    if res != 2:
        print(res)
        raise Exception("multi: mix2")
    res = r.execute_command("smembers", k3)
    if res != {"a", "c"}:
        print(res)
        raise Exception("multi: mix3")
    res = r.execute_command("zrange", k4, 0, 4)
    if res != ["one", "two"]:
        print(res)
        raise Exception("multi: mix4")



def test_all():
    empty_tran()
    simple()
    simple_with_error()
    error_but_part_success()
    evict_in_half()
    move_in_multi()
    blpop_in_multi()
    discard()
    watch_success()
    watch_fail()
    unwatch()
    mix()


def _main():
    test_all()


if __name__ == '__main__':
    _main()