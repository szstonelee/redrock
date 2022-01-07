from conn import r
import random
import time


def _main():
    hash_cnt = 0
    for i in range(1, 100001):
        key = "key" + str(i)
        val = "v" * 1000
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


if __name__ == '__main__':
    _main()