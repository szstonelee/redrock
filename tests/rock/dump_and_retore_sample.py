import redis

r1: redis.StrictRedis   # redrock
r2: redis.StrictRedis   # real redis 6.2.2

def init_redis_clients():
    r1_ip = "192.168.64.4"
    r2_ip = "192.168.64.4"
    r1_port = 6379
    r2_port = 6380
    pool1 = redis.ConnectionPool(host=r1_ip,
                                 port=r1_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='latin1',
                                 socket_connect_timeout=2)
    pool2 = redis.ConnectionPool(host=r2_ip,
                                 port=r2_port,
                                 db=0,
                                 decode_responses=True,
                                 encoding='latin1',
                                 socket_connect_timeout=2)
    r1: redis.StrictRedis = redis.StrictRedis(connection_pool=pool1)
    r2: redis.StrictRedis = redis.StrictRedis(connection_pool=pool2)
    return r1, r2


def _main():
    global r1, r2
    r1, r2 = init_redis_clients()
    key = "abc"
    cmd = f"dump {key}"
    d = r1.execute_command(cmd)
    new_d = bytes(d, "latin1")
    s = f"{new_d.__str__()[2:-1]}"
    cmd = f"restore python_abc 0 \"{s}\""
    #print(cmd)
    r1.execute_command(cmd)


if __name__ == '__main__':
    _main()