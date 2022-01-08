from conn import r


def _main():
    for i in range(1, 102):
        key = "key" + str(i)
        val = "abc"
        r.execute_command("set", key, val)


if __name__ == '__main__':
    _main()