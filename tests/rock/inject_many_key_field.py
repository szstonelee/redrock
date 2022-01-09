from conn import r
import random
import time

# please set redrock rockmaxmem to 100 M and hash-max-rock-entries = 4

def insert_first(val: str):
    str_cnt = 0
    hash_cnt = 0
    for i in range(1, 100001):
        str_cnt = str_cnt + 1
        key = "key" + str(str_cnt)
        r.execute_command("set", key, val)
        dice_for_hash = random.randint(1, 100)
        if dice_for_hash == 1:
            hash_cnt = hash_cnt + 1
            hkey = "hkey" + str(hash_cnt)
            field_num = random.randint(1, 200)
            for j in range(1, field_num+1):
                field = "f" + str(j)
                r.execute_command("hset", hkey, field, val)
        if i % 1000 == 0:
            print(f"i = {i}, time = {time.time()}")

    return str_cnt, hash_cnt


def loop_read(str_cnt: int, hash_cnt: int, val: str):
    cnt = 0
    while True:
        is_str_choice = random.choice((True, False))
        if is_str_choice:
            index = random.randint(1, str_cnt)
            key = "key" + str(index)
            v = r.execute_command("get", key)
            if v != val:
                raise Exception(f"str value not correct! {key}, {v}")
        else:
            index = random.randint(1, hash_cnt)
            key = "hkey" + str(index)
            exist = r.execute_command("exists", key)
            if not exist:
                raise Exception(f"hask key not exist, {key}")
            index = random.randint(1, 200)
            field = "f" + str(index)
            v = r.execute_command("hget", key, field)
            if v is not None:
                if v != val:
                    raise Exception(f"hash value failed, {key}, {field}")

        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"cnt = {cnt}, time = {time.time()}")


def _main():
    val = "v" * 1000
    str_cnt, hash_cnt = insert_first(val)
    loop_read(int(str_cnt/2), int(hash_cnt/2), val)


if __name__ == '__main__':
    _main()