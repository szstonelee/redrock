import sys
import time

import redis
import random

# This is for a simple sample test for RedRock for Readme.md
# It will generage some string keys in database 0 of RedRock

r_ip = "192.168.56.20"
r_port = 6379
pool = redis.ConnectionPool(host=r_ip,
                            port=r_port,
                            db=0,
                            decode_responses=True,
                            encoding='latin1',
                            socket_connect_timeout=2)
r: redis.StrictRedis = redis.StrictRedis(connection_pool=pool)


def _main():
    num = int(sys.argv[1])
    if num < 0:
        exit(1)
    for i in range(1, num+1):
        key = "k" + str(i)
        val_len = random.randint(2, 2000)
        val = str(val_len) + "v" * val_len
        cmd = f"set {key} {val}"
        r.execute_command(cmd)
        if i % 1000 == 1:
            print(f"gen key i = {i}, time = {time.time()}")
    r.execute_command("save")
    print(f"Generate {num} string keys in RedRock success! Now you can use dump.rdb.")


if __name__ == '__main__':
    _main()

