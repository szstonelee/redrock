import sys


def gen_mass_file(key_num: int, file_name):
    f = open(file_name, "x")
    for i in range(1, key_num+1):
        f.write(f"SET k{i} {'v'*1000}\r\n")
    f.close()


def _main():
    if len(sys.argv) != 3:
        print("Usage: python3 gen_mass_file.py <key_number> <output_file_name>")
        return

    try:
        key_num = int(sys.argv[1])
        if key_num < 0:
            print("the first argument must be positive!")
            return
        file_name = sys.argv[2]
        gen_mass_file(key_num, file_name)
    except ValueError:
        print("the first argument must be integer!")
        return


if __name__ == '__main__':
    _main()