import sys
import time
import string
import redis
import random

# This is for a simple sample test for RedRock for Readme.md
# It will generage some string keys in database 0 of RedRock

r_ip = "192.168.56.21"
r_port = 6379
pool = redis.ConnectionPool(host=r_ip,
                            port=r_port,
                            db=0,
                            decode_responses=True,
                            encoding='latin1',
                            socket_connect_timeout=2)
r: redis.StrictRedis = redis.StrictRedis(connection_pool=pool)


# if vals is None，generate fix val
def gen_strs(num: int, vals):
    for i in range(1, num+1):
        key = "str_" + str(i)
        val_len = random.randint(2, 2000)
        if vals is None:
            val = str(val_len) + "s" * val_len
        else:
            val = str(i) + "_" + random.choice(vals)
        cmd = f"set {key} {val}"
        r.execute_command(cmd)
        if i % 1000 == 1:
            print(f"gen key i = {i}, time = {time.time()}")

    print(f"Generate {num} string keys in RedRock success!")


def gen_hashs(num: int, f_num: int, vals):
    cnt = 0
    for i in range(1, num+1):
        key = "hash_" + str(i)
        for j in range(1, f_num+1):
            f_name = "f_" + str(j)
            if vals is None:
                v_len = random.randint(2, 2000)
                val = str(v_len) + "f" * v_len
            else:
                val = str(i) + "_" + str(j) + "_" + random.choice(vals)
            cmd = f"hset {key} {f_name} {val}"
            r.execute_command(cmd)
            cnt = cnt + 1
            if cnt % 1000 == 1:
                print(f"gen hash cnt = {cnt}, time = {time.time()}")

    print(f"Generate {num} hash keys with total fields = {cnt} in RedRock success!")


def get_random_vals():
    vals = []
    val = ""
    candidates = string.ascii_letters
    for _ in range(0, random.randint(20, 2000)):
        val = val + random.choice(candidates)
        vals.append(val)
    return tuple(vals)


def _main():
    argv_num: int = len(sys.argv)
    script_name: str = sys.argv[0]

    if argv_num < 2:
        print(f"python3 {script_name} <type> ...")
        exit(1)

    gen_type = sys.argv[1]
    vals = get_random_vals()

    if gen_type == "str":
        if argv_num < 3:
            print(f"python3 {script_name} {gen_type} <num>")
            exit(1)

        str_num = int(sys.argv[2])
        if str_num < 0:
            print(f"num can not be negative or zero")
            exit(1)

        gen_strs(str_num, vals)

    elif gen_type == "hash":
        if argv_num < 4:
            print(f"python3 {script_name} {gen_type} <hash_num> <field_num>")
            exit(1)

        hash_num = int(sys.argv[2])
        field_num = int(sys.argv[3])

        gen_hashs(hash_num, field_num, vals)

    else:
        print(f"no recognized type = {gen_type}")
        exit(1)


if __name__ == '__main__':
    _main()

