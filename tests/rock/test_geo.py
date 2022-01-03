from conn import r, rock_evict


key = "_test_rock_geo_"


def geoadd():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one")
    rock_evict(key)
    res = r.execute_command("geoadd", key, 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
    if res != 2:
        print(res)
        raise Exception("geoadd fail")


def geohash():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one")
    r.execute_command("geoadd", key, 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
    rock_evict(key)
    res = r.execute_command("geohash", key, "Palermo", "Catania")
    if res != ['sqc8b49rny0', 'sqdtr74hyu0']:
        print(res)
        raise Exception("geohash fail")


def geopos():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one")
    r.execute_command("geoadd", key, 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
    rock_evict(key)
    res = r.execute_command("geopos", key, "Palermo", "Catania", "NonExisting")
    if res != [(13.36138933897018433, 38.11555639549629859), (15.08726745843887329, 37.50266842333162032), None]:
        print(res)
        raise Exception("geopos fail")


def geodist():
    r.execute_command("del", key)
    r.execute_command("zadd", key, 1, "one")
    r.execute_command("geoadd", key, 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
    rock_evict(key)
    res = r.execute_command("geodist", key, "Palermo", "Catania")
    if res != 166274.1516:
        print(res)
        raise Exception("geodist fail")


def georadius():
    r.execute_command("del", key)
    r.execute_command("geoadd", key, 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
    rock_evict(key)
    res = r.execute_command("georadius", key, 15, 37, 200, "km", "WITHDIST")
    #if res != 1:
    #    print(res)


def georadiusbymember():
    r.execute_command("del", key)
    r.execute_command("geoadd", key, 13.583333, 37.316667, "Agrigento")
    r.execute_command("geoadd", key, 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
    rock_evict(key)
    res = r.execute_command("georadiusbymember", key, "Agrigento", 100, "km")
    #print(res)


def geosearch():
    r.execute_command("del", key)
    r.execute_command("geoadd", key, 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
    r.execute_command("geoadd", key, 12.758489, 38.788135, "edge1", 17.241510, 38.788135, "edge2")
    rock_evict(key)
    res = r.execute_command("geosearch", key, "FROMLONLAT", 15, 37, "BYRADIUS", 200, "km", "ASC")
    print(res)


def geosearchstore():
    r.execute_command("del", key)
    r.execute_command("geoadd", key, 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")
    r.execute_command("geoadd", key, 12.758489, 38.788135, "edge1", 17.241510, 38.788135, "edge2")
    rock_evict(key)
    other_key = key + "_other_"
    res = r.execute_command("geosearchstore", other_key, key, "FROMLONLAT", 15, 37, "BYBOX", 400, 400, "km", "ASC", "COUNT", 3)
    if res != 3:
        print(res)
        raise Exception("geosearchstore fail")


def test_all():
    geoadd()
    geohash()
    geopos()
    geodist()
    #georadius()    # Python Redis module has some bug for georadius: KeyError: 'store'. redis-cli is OK for rockevict
    #georadiusbymember()    # same problem
    #geosearch()            # same problem
    geosearchstore()


def _main():
    cnt = 0
    while (1):
        test_all()
        cnt = cnt + 1
        if cnt % 1000 == 0:
            print(f"test geo OK cnt = {cnt}")


if __name__ == '__main__':
    _main()