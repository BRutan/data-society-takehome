from functools import wraps
import inspect
import logging
import logging.config
import os
import re
import string
import yaml

def get_logger(app_name:str) -> logging.Logger:
    """
    * Retrieve logging object with config used.
    """
    cfg_path = os.path.join(os.environ["HOME"], "shared/logging.yml")
    with open(cfg_path, "r") as f:
        log_cfg = yaml.safe_load(f)
    logging.config.dictConfig(log_cfg)
    # Silence the fastexcel warnings:
    logging.getLogger("fastexcel").setLevel(logging.ERROR)
    return logging.getLogger(app_name)

log = get_logger("etl")

def log_execution(func):
    """
    * Log start and end of function using decorator.
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

def standardize_object_name(name:str) -> str:
    """
    * Standardize object name.
    """
    cleaned = re.sub(r"(\d+__|Y\d+\.xlsx)", "", name)
    cleaned = re.sub(r"^(\d+_\d+_+|_+)", "", cleaned).lower()
    cleaned = re.sub("[" + re.escape(string.punctuation) + "]", "", cleaned)
    return re.sub(r"\s+", "_", cleaned)