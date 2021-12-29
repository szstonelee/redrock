from conn import r, rock_evict


key = "_test_rock_set_"


def sadd():
    r.execute_command("del", key)
    r.sadd(key, "Hello")
    r.sadd(key, "World")
    rock_evict(key)
    r.sadd(key, "World")
    r.sadd(key, "Master")
    res = r.smembers(key)
    if res != {"Hello", "Master", "World"}:
        print(res)
        raise Exception("sadd fail")
    rock_evict(key)
    res = r.execute_command("sadd", key, "one", "two", "one")
    if res != 2:
        print(res)
        raise Exception("sadd fail2")


def sadd_int():
    r.execute_command("del", key)
    r.sadd(key, 123)
    r.sadd(key, 456)
    rock_evict(key)
    r.sadd(key, 789)
    res = r.smembers(key)
    if res != {"123", "456", "789"}:
        print(res)
        raise Exception("sadd_int fail")


def scard():
    r.execute_command("del", key)
    r.sadd(key, "Hello")
    r.sadd(key, "World")
    rock_evict(key)
    res = r.scard(key)
    if res != 2:
        print(res)
        raise Exception("scard fail")


def sdiff():
    k1 = "_k1"
    k2 = "_k2"
    k3 = "_k3"
    r.execute_command("del", k1, k2, k3)
    r.sadd(k1, "a", "b", "c", "d")
    r.sadd(k2, "c")
    r.sadd(k3, "a", "c", "e")
    rock_evict(k1, k2, k3)
    res = r.sdiff(k1, k2, k3)
    if res != {"b", "d"}:
        print(res)
        raise Exception("sdiff fail")


def sdiffstore():
    k1 = key + "_k1"
    k2 = key + "_k2"
    r.execute_command("del", key, k1, k2)
    r.sadd(k1, "a", "b", "c")
    r.sadd(k2, "c", "d", "e")
    rock_evict(k1, k2)
    r.sdiffstore(key, k1, k2)
    res = r.smembers(key)
    if res != {"a", "b"}:
        print(res)
        raise Exception("sdiffstore fail")


def sinter():
    k1 = "_k1"
    k2 = "_k2"
    k3 = "_k3"
    r.execute_command("del", k1, k2, k3)
    r.sadd(k1, "a", "b", "c", "d")
    r.sadd(k2, "c")
    r.sadd(k3, "a", "c", "e")
    rock_evict(k1, k2, k3)
    res = r.sinter(k1, k2, k3)
    if res != {"c"}:
        print(res)
        raise Exception("sinter fail")


def sinterstore():
    k1 = key + "_k1"
    k2 = key + "_k2"
    r.execute_command("del", key, k1, k2)
    r.sadd(k1, "a", "b", "c")
    r.sadd(k2, "c", "d", "e")
    rock_evict(k1, k2)
    r.sinterstore(key, k1, k2)
    res = r.smembers(key)
    if res != {"c"}:
        print(res)
        raise Exception("sinterstore fail")


def sismember():
    r.execute_command("del", key)
    r.sadd(key, "one")
    rock_evict(key)
    res = r.sismember(key, "one")
    if res != 1:
        print(res)
        raise Exception("sismember fail")
    rock_evict(key)
    res = r.sismember(key, "two")
    if res != 0:
        print(res)
        raise Exception("sismember fail2")


def smismember():
    r.execute_command("del", key)
    r.sadd(key, "one")
    r.sadd(key, "not lookup")
    rock_evict(key)
    res = r.execute_command("smismember", key, "one", "notamember")
    if res != [1, 0]:
        print(res)
        raise Exception("smismber fail")
    rock_evict(key)
    res = r.execute_command("smismember", key, "one", "notamember", "one", "notamember")
    if res != [1, 0, 1, 0]:
        print(res)
        raise Exception("smismember fail2")


def smembers():
    r.execute_command("del", key)
    r.sadd(key, "Hello")
    r.sadd(key, "World")
    rock_evict(key)
    res = r.smembers(key)
    if res != {"World", "Hello"}:
        print(res)
        raise Exception("smembers fail")


def smove():
    other_key = key + "_other"
    r.execute_command("del", key, other_key)
    r.sadd(key, "one", "two")
    r.sadd(other_key, "three")
    rock_evict(key, other_key)
    res = r.smove(key, other_key, "two")
    if res != 1:
        print(res)
        raise Exception("smove fail")
    res = r.smembers(key)
    if res != {"one"}:
        print(res)
        raise Exception("smove fail2")
    res = r.smembers(other_key)
    if res != {"two", "three"}:
        print(res)
        raise Exception("smove fail3")


def srandmember():
    r.execute_command("del", key)
    r.sadd(key, "one", "two", "three")
    all = r.smembers(key)
    rock_evict(key)
    res = r.srandmember(key, 2)     # return python list
    for item in res:
        if item not in all:
            print(item)
            print(res)
            raise Exception("srandmember fail")


def spop():
    r.execute_command("del", key)
    r.sadd(key, "one", "two", "three")
    all: set = r.smembers(key)
    rock_evict(key)
    res = r.spop(key, 2)
    for item in res:
        if item not in all:
            print(item)
            print(res)
            raise Exception("spop fail")
        all.remove(item)
    left = r.smembers(key)
    if left != all:
        print(left)
        print(all)
        raise Exception("spop fail2")


def srem():
    r.execute_command("del", key)
    r.sadd(key, "one", "two", "three")
    rock_evict(key)
    r.srem(key, "one", "four")
    res = r.smembers(key)
    if res != {"two", "three"}:
        print(res)
        raise Exception("srem fail")
    rock_evict(key)
    res = r.execute_command("srem", key, "one", "two", "one", "two")
    if res != 1:
        print(res)
        raise Exception("srem fail")


def suion():
    k1 = "_k1"
    k2 = "_k2"
    k3 = "_k3"
    r.execute_command("del", k1, k2, k3)
    r.sadd(k1, "a", "b", "d")
    r.sadd(k2, "c")
    r.sadd(k3, "a", "c", "e")
    rock_evict(k1, k2, k3)
    res = r.sunion(k1, k2, k3)
    if res != {"a", "b", "c", "d", "e"}:
        print(res)
        raise Exception("sunion fail")


def suionstore():
    k1 = key + "_k1"
    k2 = key + "_k2"
    r.execute_command("del", key, k1, k2)
    r.sadd(k1, "a", "b", "c")
    r.sadd(k2, "c", "d", "e")
    rock_evict(k1, k2)
    r.sunionstore(key, k1, k2)
    res = r.smembers(key)
    if res != {"a", "b", "c", "d", "e"}:
        print(res)
        raise Exception("suionstore fail")


def test_all():
    sadd()
    sadd_int()
    scard()
    sdiff()
    sdiffstore()
    sinter()
    sinterstore()
    sismember()
    smismember()
    smembers()
    smove()
    srandmember()
    spop()
    srem()
    suion()
    suionstore()


def _main():
    cnt = 0
    while (1):
        test_all()
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test str OK cnt = {cnt}")


if __name__ == '__main__':
    _main()