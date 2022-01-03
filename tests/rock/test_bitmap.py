from conn import r, rock_evict


key = "_test_rock_bitmap_"



def setbit():
    r.execute_command("del", key)
    rock_evict(key)
    res = r.execute_command("setbit", key, 7, 1)
    if res != 0:
        print(res)
        raise Exception("setbit fail")
    rock_evict(key)
    res = r.execute_command("setbit", key, 7, 0)
    if res != 1:
        print(res)
        raise Exception("setbit fail2")
    res = r.get(key)
    if res != "\u0000":
        print(res)
        raise Exception("setbit fail3")


def bitcount():
    r.execute_command("set", key, "foobar")
    rock_evict(key)
    res = r.execute_command("bitcount", key)
    if res != 26:
        print(res)
        raise Exception("bitcount fail")
    rock_evict(key)
    res = r.execute_command("bitcount", key, 0, 0)
    if res != 4:
        print(res)
        raise Exception("bitcount fail2")
    rock_evict(key)
    res = r.execute_command("bitcount", key, 1, 1)
    if res != 6:
        print(res)
        raise Exception("bitcount fail3")


def bitfield():
    r.execute_command("set", key, "")
    rock_evict(key)
    res = r.execute_command("bitfield", key, "INCRBY", "i5", 100, 1, "GET", "u4", 0)
    if res != [1, 0]:
        prinit(res)
        raise Exception("bitfield fail")


def bitfield_ro():
    r.execute_command("set", key, "")
    rock_evict(key)
    res = r.execute_command("bitfield_ro", key, "GET", "i8", 16)
    if res != [0]:
        print(res)
        raise Exception("bitfield_ro fail")


def bitop():
    r.execute_command("set", key, "foobar")
    other_key = key + "_other_"
    r.execute_command("set", other_key, "abcdef")
    rock_evict(key, other_key)
    dest_key = key + "_dest_"
    res = r.execute_command("bitop", "AND", dest_key, key, other_key)
    if res != 6:
        print(res)
        raise Exception("bitop fail")
    res = r.get(dest_key)
    if res != "`bc`ab":
        print(res)
        raise Exception("bitop fail2")


def bitpos():
    r.execute_command("set", key, b"\xff\xf0\x00")
    rock_evict(key)
    res = r.execute_command("bitpos", key, 0)
    if res != 12:
        print(res)
        raise Exception("bitpos fail")


def getbit():
    r.execute_command("set", key, "")
    r.execute_command("setbit", key, 7, 1)
    rock_evict(key)
    res = r.execute_command("getbit", key, 0)
    if res != 0:
        print(res)
        raise Exception("getbit fail")
    rock_evict(key)
    res = r.execute_command("getbit", key, 7)
    if res != 1:
        print(res)
        raise Exception("getbit fail2")


def test_all():
    setbit()
    bitcount()
    bitfield()
    bitfield_ro()
    bitop()
    bitpos()
    getbit()


def _main():
    cnt = 0
    while (1):
        test_all()
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test bitmap OK cnt = {cnt}")


if __name__ == '__main__':
    _main()
