import test_str, test_set, test_hash, test_list, test_zset


def _main():
    while (1):
        test_str.test_all()
        test_set.test_all()
        test_list.test_all()
        test_hash.test_all()
        test_zset.test_all()


if __name__ == '__main__':
    _main()