from functools import wraps
import inspect
import logging

log = logging.getLogger()

def log_execution(func):
    """
    * Log start end end of logs.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        log.debug(f"Starting {func.__name__}()")
        if inspect.ismethod(func):
            self = args[0]
            result = func(self, *args, **kwargs)
        else:
            result = func(*args, **kwargs)
        log.debug(f"Finished {func.__name__}()")
        return result
    return wrapper