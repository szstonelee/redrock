# It is for compare dataset in RedRock and Redis

import redis

r1: redis.StrictRedis   # redrock
r2: redis.StrictRedis   # real redis 6.2.2


def init_redis_clients(dbid):
    r1_ip = "192.168.56.3"
    r2_ip = "192.168.56.8"
    r1_port = 6379
    r2_port = 6379
    pool1 = redis.ConnectionPool(host=r1_ip,
                                 port=r1_port,
                                 db=dbid,
                                 decode_responses=True,
                                 encoding='latin1',
                                 socket_connect_timeout=2)
    pool2 = redis.ConnectionPool(host=r2_ip,
                                 port=r2_port,
                                 db=dbid,
                                 decode_responses=True,
                                 encoding='latin1',
                                 socket_connect_timeout=2)
    r1: redis.StrictRedis = redis.StrictRedis(connection_pool=pool1)
    r2: redis.StrictRedis = redis.StrictRedis(connection_pool=pool2)
    return r1, r2


def get_db_size(r: redis.StrictRedis):
    return r.execute_command("dbsize")


def compare(dbid):
    global r1, r2
    sz1 = get_db_size(r1)
    sz2 = get_db_size(r2)
    if sz1 != sz2:
        raise Exception(f"dbsize not match, dbid = {dbid}, sz1 = {sz1}, sz2 = {sz2}")
    if sz1 == 0:
        return
    print(f"dbid = {dbid}, db size = {sz1}")
    keys = r1.execute_command("keys *")
    for key in keys:
        exist = r2.execute_command(f"exists {key}")
        if not exist:
            raise Exception(f"key = {key} in RedRock, but not in redis")
        cmd = f"type {key}"
        t1 = r1.execute_command(cmd)
        t2 = r2.execute_command(cmd)
        if t1 != t2:
            raise Exception(f"key = {key} type not correct, t1 = {t1}, t2 = {t2}")
        if t1 == "string" or t1 == "zset":
            dump1 = r1.execute_command(f"dump {key}")
            dump2 = r2.execute_command(f"dump {key}")
            if dump1 != dump2:
                raise Exception(f"key = {key}, not same")
        elif t1 == "list":
            cmd = f"llen {key}"
            len1 = r1.execute_command(cmd)
            len2 = r2.execute_command(cmd)
            if len1 != len2:
                raise Exception(f"key = {key}, list len not same!")
            cmd = f"lrange {key} 0 -1"
            all1 = r1.execute_command(cmd)
            all2 = r2.execute_command(cmd)
            if all1 != all2:
                raise Exception(f"key = {key}, list content not same!")
        elif t1 == "hash" or t1 == "set":
            if t1 == "hash":
                cmd = f"hkeys {key}"
            else:
                cmd = f"smembers {key}"
            fields1: set = r1.execute_command(cmd)
            fields2: set = r2.execute_command(cmd)
            if len(fields1) != len(fields2):
                raise Exception(f"key = {key}, len not same")
            for field in fields1:
                if field not in fields2:
                    raise Exception(f"key = {key}, field = {field} in fields1 not in fields2")
                if t1 == "hash":
                    cmd = f"hget {key} {field}"
                    v1 = r1.execute_command(cmd)
                    v2 = r2.execute_command(cmd)
                    if v1 != v2:
                        raise Exception(f"key = {key}, field = {field}, value not same for a hash")
        else:
            raise Exception(f"unrecognized type for key = {key}, type = {t1}")


def _main():
    global r1, r2
    for dbid in range(0, 16):
        r1, r2 = init_redis_clients(dbid)
        compare(dbid)
    print("exactly same!!!!")


if __name__ == '__main__':
    _main()
