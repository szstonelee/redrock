from conn import r, rock_evict_hash


key = "_test_rock_hash_"


# assume a hash more than 4 fields will be in a rock hash
def build_rock_hash(k):
    r.execute_command("del", k)
    r.execute_command("hset", k, "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5", "f6", "v6")


def hdel():
    build_rock_hash(key)
    rock_evict_hash(key, "f2")
    res = r.execute_command("hdel", key, "f1")
    if res != 1:
        print(res)
        raise Exception("rock_hash:hdel")



def test_all():
    hdel()


def _main():
    test_all()


if __name__ == '__main__':
    _main()