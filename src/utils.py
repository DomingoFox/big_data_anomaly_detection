
import time
import functools
from threading import local
from logger import Logger

class TimeTracker:
    def __init__(self, output_dir=None):
        self._thread_local = local()
        self.logger = Logger(output_dir, verbose_timing=False)  # Disable verbose timing by default

    def time_tracker(self, suppress=False):
        #Decorator to track execution time of a function and log it.
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                result = func(*args, **kwargs)
                duration = time.time() - start_time

                if suppress:
                    # Aggregate timings in thread-local storage
                    if not hasattr(self._thread_local, 'timings'):
                        self._thread_local.timings = {}
                    self._thread_local.timings[func.__name__] = self._thread_local.timings.get(func.__name__, 0) + duration
                else:
                    # Only log for high-level functions like run_sequential, run_parallel
                    if func.__name__ in ['run_sequential', 'run_parallel']:
                        self.logger.info(f"Function '{func.__name__}' took {duration:.2f} seconds to execute", category="timing")

                return result
            return wrapper
        return decorator

    def log_aggregated_timings(self):
        # Log aggregated timings from parallel processing and reset.
        if hasattr(self._thread_local, 'timings'):
            for func_name, total_time in self._thread_local.timings.items():
                # Optionally log aggregated timings if needed
                if func_name in ['run_parallel']:  # Limit to specific functions if desired
                    self.logger.info(f"Function '{func_name}' total time across all threads: {total_time:.2f} seconds", category="timing")
            self._thread_local.timings = {}  # Reset after logging

time_tracker_instance = TimeTracker()

# Expose functions for easier import
time_tracker = time_tracker_instance.time_tracker
log_aggregated_timings = time_tracker_instance.log_aggregated_timings