from concurrent.futures import ProcessPoolExecutor, as_completed
from random import random
from time import sleep


def task(idx):
    time = int(random() * 5)
    sleep(time)
    return idx, time


def main():
    futures = []
    with ProcessPoolExecutor(max_workers=3) as executor:
        for idx in range(10):
            future = executor.submit(task, idx)
            futures.append(future)

        for future in as_completed(futures):
            idx, time = future.result()
            print(f"Task {idx}: slept {time}")
            futures.remove(future)


if __name__ == "__main__":
    main()
