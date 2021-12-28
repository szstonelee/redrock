import test_str, test_set, test_hash, test_list, test_zset, test_bitmap, test_geo, test_hyperloglog, test_multi


def _main():
    cnt = 0
    while (1):
        #test_str.test_all()    # have sleep
        test_set.test_all()
        test_list.test_all()
        test_hash.test_all()
        test_zset.test_all()
        test_bitmap.test_all()
        test_geo.test_all()
        test_hyperloglog.test_all()
        #test_multi.test_all()
        cnt = cnt + 1
        if cnt % 100 == 0:
            print(f"test loop for set, list, hash, zset, bitmap, geo, hll OK cnt = {cnt}")


if __name__ == '__main__':
    _main()