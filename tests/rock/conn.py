import redis

#redis_ip = "127.0.0.1"
redis_ip = "192.168.56.3"
redis_port = 6379

pool = redis.ConnectionPool(host=redis_ip,
                            port=redis_port,
                            db=0,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)

r: redis.StrictRedis = redis.StrictRedis(connection_pool=pool)

# r2: redis.StrictRedis = redis.StrictRedis(connection_pool=pool)


def rock_evict(*keys):
    r.execute_command("rockevict", *keys)


def rock_evict_hash(key, *fields):
    r.execute_command("rockevicthash", key, *fields)


def _main():
    r.set(name="k1", value="123")
    # print(r.get(name="k1"))
    cmd = "rockevict"
    k1 = "k1"
    k2 = "k2"
    print(r.execute_command(cmd, k1, k2))


if __name__ == '__main__':
    _main()