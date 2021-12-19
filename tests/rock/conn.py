import redis

redis_ip = "192.168.64.4"
redis_port = 6379

pool = redis.ConnectionPool(host=redis_ip,
                            port=redis_port,
                            db=0,
                            decode_responses=True,
                            encoding='utf-8',
                            socket_connect_timeout=2)

r: redis.StrictRedis = redis.StrictRedis(connection_pool=pool)


def rock_evict(*keys):
    r.execute_command("rockevict", *keys)


def _main():
    r.set(name="k1", value="123")
    # print(r.get(name="k1"))
    cmd = "rockevict"
    k1 = "k1"
    k2 = "k2"
    print(r.execute_command(cmd, k1, k2))


if __name__ == '__main__':
    _main()