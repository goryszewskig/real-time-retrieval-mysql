"""
Open-loop load generator, shared by read-benchmarking.py and
write-benchmarking.py
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor


def run_open_loop(
    rate,
    duration,
    task_fn,
    on_success,
    on_error,
    max_concurrency,
    worker_init=None,
):
    """
    Dispatch task_fn() at the target rate (calls/sec) for
    `duration` seconds, using up to max_concurrency concurrent
    workers to actually run it.

    task_fn() takes no arguments (close over whatever per-call
    inputs are needed) and returns a result or raises.

    on_success(result) / on_error(exc) run on the worker thread
    that finished the call - they're responsible for their own
    locking if they touch shared state.

    worker_init(), if given, runs once per worker thread before it
    processes any tasks (e.g. to open a per-thread connection and
    stash it in thread-local storage - needed for clients, like
    psycopg2, that aren't safe to share across threads).

    A semaphore bounds how many calls are in flight at once: if
    max_concurrency is too low to sustain `rate` given how long
    task_fn() actually takes, the scheduling loop itself blocks
    and the dispatch rate drops below `rate` - an observable
    signal that concurrency needs to go up, rather than an
    ever-growing hidden backlog of queued-but-not-yet-run calls.

    Returns (dispatched_count, actual_duration_seconds).
    """

    executor = ThreadPoolExecutor(
        max_workers=max_concurrency,
        initializer=worker_init,
    )

    in_flight = threading.Semaphore(max_concurrency)

    def _run_one():
        try:
            result = task_fn()
        except Exception as exc:
            on_error(exc)
        else:
            on_success(result)
        finally:
            in_flight.release()

    interval = 1.0 / rate

    start_time = time.perf_counter()
    next_tick = start_time

    dispatched = 0

    while True:
        now = time.perf_counter()

        if now - start_time >= duration:
            break

        if now < next_tick:
            time.sleep(next_tick - now)

        next_tick += interval

        in_flight.acquire()
        executor.submit(_run_one)
        dispatched += 1

    # Let in-flight calls finish before returning, so callers can
    # rely on on_success/on_error having been called for every
    # dispatched task by the time this returns.
    executor.shutdown(wait=True)

    actual_duration = time.perf_counter() - start_time

    return dispatched, actual_duration
