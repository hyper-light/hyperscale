import time

def run():

    iters = 10**9
    start = time.monotonic()

    [
        x for x in range(iters) for y in range(iters)
    ]

    elapsed = time.monotonic() - start


run()