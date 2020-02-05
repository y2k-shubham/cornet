from typing import List, Tuple


def generate_wait_times(count: int, max_wait: int = 10) -> List[Tuple[int, int]]:
    from random import randint
    wait_times: List[Tuple[int, int]] = []
    for i in range(count):
        wait_times.append((i, randint(0, max_wait)))
    return wait_times


def sleep_thread(seconds: int) -> None:
    from time import sleep
    sleep(seconds)


def perform_wait(tup: Tuple[int, int]) -> None:
    print('sleep#{}={}'.format(tup[0], tup[1]))
    sleep_thread(tup[1])


def perform_parallel_sleep(wait_times: List[Tuple[int, int]], pool_size: int = 4) -> None:
    from multiprocessing.dummy import Pool
    from multiprocessing.pool import ThreadPool
    pool: ThreadPool = Pool(pool_size)

    pool.map(perform_wait, wait_times)


def main() -> None:
    wait_times: List[Tuple[int, int]] = generate_wait_times(count=20, max_wait=5)
    perform_parallel_sleep(wait_times=wait_times, pool_size=1)


if __name__ == '__main__':
    main()
