from conn import r, rock_evict


key = "_test_rock_zset_"


def zadd():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one")
    rock_evict(key)
    r.execute_command("zadd", key, 1, "uno")
    rock_evict(key)
    r.execute_command("zadd", key, 2,"two", 3, "three")
    res = r.zrange(key, 0, -1, withscores=1)
    if res != [('one', 1.0), ('uno', 1.0), ('two', 2.0), ('three', 3.0)]:
        print(res)
        raise Exception("zddd fail")


def zcard():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one")
    r.execute_command("zadd", key, 2, "two")
    rock_evict(key)
    res = r.execute_command("zcard", key)
    if res != 2:
        print(res)
        raise Exception("zcard fail")


def zcount():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one")
    r.execute_command("zadd", key, 2, "two")
    r.execute_command("zadd", key, 3, "three")
    rock_evict(key)
    res = r.execute_command("zcount", key, "-inf", "+inf")
    if res != 3:
        print(res)
        raise Exception("zcount fail")
    rock_evict(key)
    res = r.execute_command("zcount", key, "(1", 3)
    if res != 2:
        print(res)
        raise Exception("zcount fail2")


def zdiff():
    r.execute_command("del", key)
    other_key = key + "_other"
    r.execute_command("zadd", key, 1, "one")
    r.execute_command("zadd", key, 2, "two")
    r.execute_command("zadd", key, 3, "three")
    r.execute_command("zadd", other_key, 1, "one")
    r.execute_command("zadd", other_key, 2, "two")
    rock_evict(key, other_key)
    res = r.execute_command("zdiff", 2, key, other_key)
    if (res != ["three"]):
        print(res)
        raise Exception("zdiff fail")


def test_all():
    zadd()
    zcard()
    zcount()
    zdiff()


def _main():
    test_all()


if __name__ == '__main__':
    _main()