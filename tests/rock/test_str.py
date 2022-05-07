from conn import r, rock_evict
import time


key = "_test_rock_str_"


def append():
    original_val = "abc"
    append_val = "_append"
    r.set(key, original_val)
    rock_evict(key)
    r.append(key, append_val)
    check = r.get(key)
    if check != original_val + append_val:
        print(check)
        raise Exception("append fail")


def append_long_str():
    original_val = "abc"
    append_val = "_append" * 100
    r.set(key, original_val)
    rock_evict(key)
    r.append(key, append_val)
    check = r.get(key)
    if check != original_val + append_val:
        print(check)
        raise Exception("append_long_str fail")

# LONG_MAX 9223372036854775807
def decr():
    original_num = 9223372036854775807
    r.set(key, original_num)
    rock_evict(key)
    res = r.execute_command("decr", key)
    if res != original_num - 1:
        print(res)
        raise Exception("decr fail")
    small_num = 83423932424
    r.set(key, small_num)
    rock_evict(key)
    res = r.execute_command("decr", key)
    if res != small_num - 1:
        print(res)
        raise Exception("decr fail2")
    share_int = 7
    r.set(key, share_int)   # can not be evicted because it is shared object
    rock_evict(key)
    res = r.execute_command("decr", key)
    if res != share_int - 1:
        print(res)
        raise Exception("decr fail3")


def decrby():
    original_num = 9223372036854775807
    r.set(key, original_num)
    decrement = 123
    rock_evict(key)
    res = r.decrby(key, decrement)
    if res != original_num - decrement:
        print(res)
        raise Exception("decrby fail")


def get():
    val = "abc_test_for_get"
    r.set(key, val)
    rock_evict(key)
    res = r.get(key)
    if res != val:
        print(res)
        raise Exception("get fail")


def getdel():
    val = "uu_random_anything_"
    r.set(key, val)
    rock_evict(key)
    res = r.execute_command("getdel", key)  # only for 6.2
    if res != val:
        print(res)
        raise Exception("getdel fail")
    exist = r.get(key)
    if exist is not None:
        print(exist)
        raise Exception("getdel fail2")


def getex():
    val = "hello"
    r.set(key, val)
    rock_evict(key)
    res = r.execute_command("getex", key, "ex", "1")
    if res != val:
        print(res)
        raise Exception("getex fail")
    time.sleep(2)
    res = r.get(key)
    if res is not None:
        print(res)
        raise Exception("getex fail")


def getset():
    original_val = "ddd"
    r.set(key, original_val)
    new_val = "ppp"
    rock_evict(key)
    res = r.getset(key, new_val)
    if res != original_val:
        print(res)
        raise Exception("getset fail")
    res = r.get(key)
    if res != new_val:
        print(res)
        raise Exception("getset fail2")


def incr():
    num = 342340429389223
    r.set(key, num)
    rock_evict(key)
    res = r.execute_command("incr", key)
    if res != num + 1:
        print(res)
        raise Exception("incr fail")


def incrby():
    num = 32492384
    r.set(key, num)
    rock_evict(key)
    increment = 321
    res = r.incrby(key, increment)
    if res != num + increment:
        print(res)
        raise Exception("incrby fail")


def incrbyfloat():
    num = 10.5
    cmd = f"set {key} {num}"
    r.execute_command(cmd)
    rock_evict(key)
    increment = 0.1
    cmd = f"incrbyfloat {key} {increment}"
    res = r.execute_command(cmd)
    if float(res) != num + increment:
        print(res)
        raise Exception("incrbyfloat fail")


def mget():
    k1 = key + "_k1"
    v1 = "v1"
    r.set(k1, v1)
    k2 = key + "_k2"
    v2 = 2
    r.set(k2, v2)
    k3 = key + "_k3"
    v3 = 10.1
    r.set(k3, v3)
    k4 = key + "_k4"
    r.execute_command("del", k4)
    rock_evict(k1, k2, k3, k4)
    res = r.mget(k1, k2, k3, k4)
    if res != [v1, str(v2), str(v3), None]:
        print(res)
        raise Exception("mget fail")
    rock_evict(k1, k2, k3, k4)
    res = r.execute_command("mget", k1, k1, k2, k2, k3, k3, k4, k4)
    if res != ["v1", "v1", "2", "2", "10.1", "10.1", None, None]:
        print(res)
        raise Exception("mget fail2")


def mset():
    k1 = key + "_k1"
    v1 = "v1"
    k2 = key + "_k2"
    v2 = 2
    k3 = key + "_k3"
    v3 = 10.3
    k4 = key + "_k4"
    r.execute_command("del", k4)
    r.set(k1, v1)
    r.set(k2, v2)
    r.set(k3, v3)
    rock_evict(k1, k2, k3, k4)
    r.execute_command("mset", k1, v1, k2, v2, k1, v1, k3, v2, k3, v3)
    res = r.execute_command("mget", k1, k2, k3, k4)
    if res != [v1, str(v2), str(v3), None]:
        print(res)
        raise Exception("mset fail")


def psetex():
    val = "abc"
    r.set(key, val)
    rock_evict(key)
    r.psetex(key, 500, val)
    res = r.get(key)
    if res != val:
        print(res)
        raise Exception("psetex fail")
    rock_evict(key)
    time.sleep(2)
    res = r.get(key)
    if res is not None:
        print(res)
        raise Exception("psetex fail2")


def set():
    old_val = "hhh"
    r.set(key, old_val)
    rock_evict(key)
    new_val = "pxp"
    r.execute_command("set", key, new_val, "ex", "1")
    res = r.get(key)
    if res != new_val:
        print(res)
        raise Exception("set fail")
    time.sleep(2)
    res = r.get(key)
    if res is not None:
        print(res)
        raise Exception("set fail2")


def setex():
    val = "abc"
    r.set(key, val)
    rock_evict(key)
    new_val = "tx-"
    r.setex(key, "1", new_val)
    res = r.get(key)
    if res != new_val:
        print(res)
        raise Exception("setex fail")
    time.sleep(2)
    res = r.get(key)
    if res is not None:
        print(res)
        raise Exception("setex fail2")


def setrange():
    val = "Hello World"
    r.set(key, val)
    rock_evict(key)
    r.setrange(key, "6", "Redis")
    res = r.get(key)
    if res != "Hello Redis":
        print(res)
        raise Exception("setrange fail")


def strlen():
    val = "akdreirjew"
    r.set(key, val)
    rock_evict(key)
    res = r.strlen(key)
    if res != len(val):
        print(res)
        raise Exception("strlen fail")


def test_all():
    append()
    append_long_str()
    decr()
    decrby()
    get()
    getdel()
    getex()
    getset()
    incr()
    incrby()
    incrbyfloat()
    mget()
    mset()
    psetex()
    set()
    setex()
    setrange()
    strlen()


def _main():
    cnt = 0
    while (1):
        test_all()
        cnt = cnt + 1
        if cnt % 10 == 0:
            print(f"test str OK cnt = {cnt}")


if __name__ == '__main__':
    _main()


