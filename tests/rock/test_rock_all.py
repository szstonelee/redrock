from conn import r


def _main():
    for i in range(1, 3000):
        key = "more_key" + str(i)
        val = "abc" * 30000
        r.execute_command("set", key, val)


if __name__ == '__main__':
    _main()