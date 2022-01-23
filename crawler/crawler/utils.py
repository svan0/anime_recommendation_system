from functools import wraps
import time

def crawler_stats_timing(func):
    @wraps(func)
    def crawler_stats_timing_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        args[0].stats.inc_value(f'{args[0].__class__.__name__}_execution_time', count = total_time)
        return result
    return crawler_stats_timing_wrapper