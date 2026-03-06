import time
from concurrent.futures import ProcessPoolExecutor


def main():
    print("Hello World")
    raise ValueError("Hello World")
if __name__ == '__main__':

    with ProcessPoolExecutor() as pool:
        f = pool.submit(main)
        time.sleep(5)