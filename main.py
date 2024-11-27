from datetime import datetime

from fake_useragent import UserAgent


def main():
    pass


if __name__ == '__main__':
    ua = UserAgent()
    print(ua.chrome)
    print(ua.chrome)
    print(ua.chrome)
    print(filename1 := f"goods_raw_{datetime.now().strftime("%Y%m%d-%H%M%S%f")}.csv")
    main()
