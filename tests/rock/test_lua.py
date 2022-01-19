from conn import r, rock_evict

key = "test_lua"


def init():
    r.execute_command("config set hash-max-ziplist-entries 2")
    r.execute_command("config set hash-max-rock-entries 4")
    k1 = key + "_1"
    k2 = key + "_2"
    r.execute_command("del", k1)
    r.execute_command("del", k2)
    r.execute_command("hmset", k1, "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5")
    r.execute_command("set", k2, "val")
    r.execute_command("rockevicthash", k1, "f2")


def lua():
    mylua = """
    local k1 = KEYS[1]
    local k2 = KEYS[2]
    local f = ARGV[1]
    local append_str = ARGV[2]    
    local res1 = redis.call("append", k2, append_str)
    local res2 = redis.call("hget", k1, f)
    return {res1, res2}
    """
    k1 = key + "_1"
    k2 = key + "_2"
    cmd = r.register_script(mylua)
    res = cmd(keys=[k1, k2], args = ["f2", "_add_something"])
    if res != [17, "v2"]:
        print(res)
        raise Exception("lua")


def test_all():
    init()
    lua()


def _main():
    cnt = 0
    while (1):
        test_all()
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test list OK cnt = {cnt}")


if __name__ == '__main__':
    _main()
