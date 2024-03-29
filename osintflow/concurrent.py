import traceback
from concurrent.futures import ThreadPoolExecutor

import osintflow.config as config


class _Concurrent:
    def __init__(self):
        self.pool = ThreadPoolExecutor(max_workers=config.max_workers, thread_name_prefix='osintflow_concurrent_')

    def _wait(self):
        self.pool.shutdown()

    def thread(self, callback=None):
        def wrapper(func):
            def inner_wrapper(*args, **kwargs):
                def task():
                    try:
                        data = func(*args, **kwargs)
                        if callback is not None:
                            callback(data)
                    except Exception as e:
                        traceback.print_exc()

                return self.pool.submit(task)

            return inner_wrapper

        return wrapper


_concurrent = _Concurrent()
wait = _concurrent._wait
