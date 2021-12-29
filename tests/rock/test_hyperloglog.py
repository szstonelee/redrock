from conn import r, rock_evict


key = "_test_rock_hyperloglog_"


def pfadd():
    r.execute_command("del", key)
    r.execute_command("pfadd", key, "a", "b", "c")
    rock_evict(key)
    r.execute_command("pfadd", key, "d", "e", "f", "g")
    res = r.execute_command("pfcount", key)
    if res != 7:
        print(res)
        raise Exception("pfadd fail")


def pfcount():
    r.execute_command("del", key)
    r.execute_command("pfadd", key, "foo", "bar", "zap")
    r.execute_command("pfadd", key, "zap", "zap", "zap")
    r.execute_command("pfadd", key, "foo", "bar")
    rock_evict(key)
    res = r.execute_command("pfcount", key)
    if res != 3:
        print(res)
        raise Exception("pfcount fail")
    other_key = key + "_other_"
    r.execute_command("del", other_key)
    r.execute_command("pfadd", other_key, 1, 2, 3)
    rock_evict(key, other_key)
    res = r.execute_command("pfcount", key, other_key)
    if res != 6:
        print(res)
        raise Exception("pfcount fail2")


def pfmerge():
    k1 = key + "_k1"
    k2 = key + "_k2"
    k3 = key + "_k3"
    r.execute_command("del", k1, k2, k3)
    r.execute_command("pfadd", k1, "foo", "bar", "zap", "a")
    r.execute_command("pfadd", k2, "a", "b", "c", "foo")
    rock_evict(k1, k2, k3)
    r.execute_command("pfmerge", k3, k1, k2)
    res = r.execute_command("pfcount", k3)
    if res != 6:
        print(res)
        raise Exception("pfmerge fail")


def test_all():
    pfadd()
    pfcount()
    pfmerge()


def _main():
    cnt = 0
    while (1):
        test_all()
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test str OK cnt = {cnt}")


if __name__ == '__main__':
    _main()