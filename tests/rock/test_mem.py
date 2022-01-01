from conn import r, rock_evict
import sys


def hlen(key):
    r.execute_command("del", key)
    r.execute_command("hset", key, "field1", "foo", "field2", "bar")
    rock_evict(key)
    res = r.hlen(key)
    if res != 2:
        print(res)
        raise Exception("hlen fail")


def hexists(key):
    r.execute_command("del", key)
    r.hset(key, "field1", "foo")
    rock_evict(key)
    res = r.hexists(key, "field1")
    if res != 1:
         print(res)
         raise Exception("hexists fail")


def _main(key):
    cnt = 0
    while (1):
        hexists(key)
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test str OK cnt = {cnt}")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("use key")
        exit()
    key = sys.argv[1]
    print(f"key = {key}")
    _main(key)