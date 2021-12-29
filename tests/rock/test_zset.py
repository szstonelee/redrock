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
    other_key = key + "_other"
    r.execute_command("del", key, other_key)
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


def zdiffstore():
    other_key = key + "_other"
    dest_key = key + "_dest"
    r.execute_command("del", key, other_key, dest_key)
    r.execute_command("zadd", key, 1, "one")
    r.execute_command("zadd", key, 2, "two")
    r.execute_command("zadd", key, 3, "three")
    r.execute_command("zadd", other_key, 1, "one")
    r.execute_command("zadd", other_key, 2, "two")
    rock_evict(key, other_key)
    res = r.execute_command("zdiffstore", dest_key, 2, key, other_key)
    if res != 1:
        print(res)
        raise Exception("zdiffstore fail")
    res = r.execute_command("zrange", dest_key, 0, -1,"withscores")
    if res != ["three", "3"]:
        print(res)
        raise Exception("zdiffstore fail2")


def zincrby():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two")
    rock_evict(key)
    res = r.execute_command("zincrby", key, 2, "one")
    if res != 3:
        print(res)
        raise Exception("zincrby fail")


def zinter():
    other_key = key + "_other"
    r.execute_command("del", key, other_key)
    r.execute_command("zadd", key, 1, "one", 2, "two")
    r.execute_command("zadd", other_key, 1, "one", 2, "two", 3, "three")
    rock_evict(key, other_key)
    res = r.execute_command("zinter", 2, key, other_key)
    if res != ["one", "two"]:
        print(res)
        raise Exception("zinter fail")


def zinterstore():
    other_key = key + "_other"
    dest_key = key + "_dest"
    r.execute_command("del", key, other_key, dest_key)
    r.execute_command("zadd", key, 1, "one", 2, "two")
    r.execute_command("zadd", other_key, 1, "one", 2, "two", 3, "three")
    rock_evict(key, other_key, dest_key)
    res = r.execute_command("zinterstore", dest_key, 2, key, other_key, "weights", 2, 3)
    if res != 2:
        print(res)
        raise Exception("zinterstore fail")
    res = r.execute_command("zrange", dest_key, 0, -1, "withscores")
    if res != ["one", "5", "two", "10"]:
        print(res)
        raise Exception("zinterstore fail2")


def zlexcount():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 0, "a", 0, "b", 0, "c", 0, "d", 0, "e")
    r.execute_command("zadd", key, 0, "f", 0, "g")
    rock_evict(key)
    res = r.execute_command("zlexcount", key, "-", "+")
    if res != 7:
        print(res)
        raise Exception("zlexcount fail")
    res = r.execute_command("zlexcount", key, "[b", "[f")
    if res != 5:
        print(res)
        raise Exception("zlexcount fail2")


def zpopmax():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one")
    r.execute_command("zadd", key, 2, "two")
    r.execute_command("zadd", key, 3, "three")
    rock_evict(key)
    res = r.execute_command("zpopmax", key)
    if res != ["three", "3"]:
        print(res)
        raise Exception("zpopmax fail")


def zpopmin():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one")
    r.execute_command("zadd", key, 2, "two")
    r.execute_command("zadd", key, 3, "three")
    rock_evict(key)
    res = r.execute_command("zpopmin", key)
    if res != ["one", "1"]:
        print(res)
        raise Exception("zpopmin fail")


def zrandmember():
    r.execute_command("del", key)
    val = {"uno": 1, "due": 2, "tre": 3, "quattro": 4, "cinque": 5, "se": 6}
    r.zadd(key, val)
    rock_evict(key)
    res = r.execute_command("zrandmember", key)
    if res not in val:
        print(res)
        raise Exception("zrandmember fail")
    rock_evict(key)
    res2 = r.execute_command("zrandmember", key)
    if res2 not in val:
        print(res2)
        raise Exception("zrandmember fail2")


def zrangestore():
    dest_key = key + "_dest_"
    r.execute_command("del", key, dest_key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three", 4, "four")
    rock_evict(key)
    res = r.execute_command("zrangestore", dest_key, key, 2, -1)
    if res != 2:
        print(res)
        raise Exception("zrangestore fail")
    res = r.execute_command("zrange", dest_key, 0, -1)
    if res != ["three", "four"]:
        print(res)
        raise Exception("zrangestore fail2")


def zrange():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    res = r.execute_command("zrange", key, 0, -1)
    if res != ["one", "two", "three"]:
        print(res)
        raise Exception("zrange fail")
    rock_evict(key)
    res = r.execute_command("zrange", key, 2, 3)
    if res != ["three"]:
        print(res)
        raise Exception("zrange fail2")


def zrangebylex():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 0, "a", 0, "b", 0, "c", 0, "d", 0, "e")
    r.execute_command("zadd", key, 0, "f", 0, "g")
    rock_evict(key)
    res = r.execute_command("zrangebylex", key, "-", "[c")
    if res != ["a", "b", "c"]:
        print(res)
        raise Exception("zrangebylex fail")
    rock_evict(key)
    res = r.execute_command("zrangebylex", key, "-", "(c")
    if res != ["a", "b"]:
        print(res)
        raise Exception("zrangebylex fail2")
    rock_evict(key)
    res = r.execute_command("zrangebylex", key, "[aaa", "(g")
    if res != ["b", "c", "d", "e", "f"]:
        print(res)
        raise Exception("zranngebylex fail3")


def zrangebyscore():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    res = r.execute_command("zrangebyscore", key, "-inf", "+inf")
    if res != ["one", "two", "three"]:
        print(res)
        raise Exception("zrangebyscore fail")
    rock_evict(key)
    res = r.execute_command("zrangebyscore", key, 1, 2)
    if res != ["one", "two"]:
        print(res)
        raise Exception("zrangebyscore fail2")


def zrank():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    res = r.execute_command("zrank", key, "three")
    if res != 2:
        print(res)
        raise Exception("zrank fail")


def zrem():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    r.execute_command("zrem", key, "two")
    res = r.execute_command("zrange", key, 0, -1, "withscores")
    if res != ["one", "1", "three", "3"]:
        print(res)
        raise Exception("zrem fail")


def zremrangebylex():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 0, "aaaa", 0, "b", 0, "c", 0, "d", 0, "e")
    r.execute_command("zadd", key, 0, "foo", 0, "zap", 0, "zip", 0, "ALPHA", 0, "alpha")
    rock_evict(key)
    res = r.execute_command("zremrangebylex", key, "[alpha", "[omega")
    if res != 6:
        print(res)
        raise Exception("zremrangebylex fail")
    res = r.execute_command("zrange", key, 0, -1)
    if res != ["ALPHA", "aaaa", "zap", "zip"]:
        print(res)
        raise Exception("zremrangebylex fail")


def zremrangebyrank():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    r.execute_command("zremrangebyrank", key, 0, 1)
    res = r.execute_command("zrange", key, 0, -1, "withscores")
    if res != ["three", "3"]:
        print(res)
        raise Exception("zremrangebyrank fail")


def zremrangebyscore():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    r.execute_command("zremrangebyscore", key, "-inf", "(2")
    res = r.execute_command("zrange", key, 0, -1, "withscores")
    if res != ["two", "2", "three", "3"]:
        print(res)
        raise Exception("zremrangebyscore fail")


def zrevrange():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    res = r.execute_command("zrevrange", key, 0, -1)
    if res != ["three", "two", "one"]:
        print(res)
        raise Exception("zrevrange fail")


def zrevrangebyscore():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    res = r.execute_command("zrevrangebyscore", key, "2", "(1")
    if res != ["two"]:
        print(res)
        raise Exception("zrevrangebyscore fail")


def zrevrank():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    res = r.execute_command("zrevrank", key, "one")
    if res != 2:
        print(res)
        raise Exception("zrevrank fail")


def zscore():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two", 3, "three")
    rock_evict(key)
    res = r.execute_command("zscore", key, "two")
    if res != 2:
        print(res)
        raise Exception("zscore fail")


def zunion():
    other_key = key + "_other"
    r.execute_command("del", key, other_key)
    r.execute_command("zadd", key, 1, "one", 2, "two")
    r.execute_command("zadd", other_key, 1, "one", 2, "two", 3, "three")
    rock_evict(key, other_key)
    res = r.execute_command("zunion", 2, key, other_key)
    if res != ["one", "three", "two"]:
        print(res)
        raise Exception("zunion fail")


def zmscore():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one", 2, "two")
    rock_evict(key)
    res = r.execute_command("zmscore", key, "one", "two", "nofield")
    if res != [1, 2, None]:
        print(res)
        raise Exception("zmscore fail")


def zunionstore():
    other_key = key + "_other"
    dest_key = key + "_dest"
    r.execute_command("del", key, other_key, dest_key)
    r.execute_command("zadd", key, 1, "one", 2, "two")
    r.execute_command("zadd", other_key, 1, "one", 2, "two", 3, "three")
    rock_evict(key, other_key)
    res = r.execute_command("zunionstore", dest_key, 2, key, other_key, "weights", 2, 3)
    if res != 3:
        print(res)
        raise Exception("zunionstore fail")
    res = r.execute_command("zrange", dest_key, 0, -1, "withscores")
    if res != ["one", "5", "three", "9", "two", "10"]:
        print(res)
        raise Exception("zunionstore fail2")


def bzpopmin():
    other_key = key + "_other"
    r.execute_command("del", key, other_key)
    r.execute_command("zadd", key, 0, "a", 1, "b", 2, "c")
    rock_evict(key, other_key)
    res = r.execute_command("bzpopmin", key, other_key, 2)
    if res != (key, "a", 0):
        print(res)
        raise Exception("bzpopmin fail")


def bzpopmax():
    other_key = key + "_other"
    r.execute_command("del", key, other_key)
    r.execute_command("zadd", key, 0, "a", 1, "b", 2, "c")
    rock_evict(key, other_key)
    res = r.execute_command("bzpopmax", key, other_key, 2)
    if res != (key, "c", 2):
        print(res)
        raise Exception("bzpopmin fail")


def test_all():
    zadd()
    zcard()
    zcount()
    zdiff()
    zdiffstore()
    zincrby()
    zinter()
    zinterstore()
    zlexcount()
    zpopmax()
    zpopmin()
    zrandmember()
    zrangestore()
    zrange()
    zrangebylex()
    zrangebyscore()
    zrank()
    zrem()
    zremrangebylex()
    zremrangebyrank()
    zremrangebyscore()
    zrevrange()
    zrevrangebyscore()
    zrevrank()
    zscore()
    zunion()
    zmscore()
    zunionstore()
    bzpopmin()
    bzpopmax()


def _main():
    cnt = 0
    while (1):
        test_all()
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test str OK cnt = {cnt}")


if __name__ == '__main__':
    _main()