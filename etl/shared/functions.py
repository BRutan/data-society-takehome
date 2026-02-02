from functools import wraps
import inspect
import logging
import logging.config
import os
import re
import requests
import string
import yaml
import zipfile

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

def download_unzip_file(link:str, out_dir:str, log:logging.Logger):
    """
    * Download the file using streaming.
    """
    with requests.get(link, stream=True) as r:
        zip_path = re.search(r"(?P<f>\d+\.zip)$", link)["f"]
        zip_path = os.path.join(out_dir, zip_path)
        log.debug("Downloading file from %s to %s.", link, zip_path)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        # Unzip the file:
        zip_out_dir = zip_path.replace(".zip", "")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(zip_out_dir)
        os.remove(zip_path)
        return zip_out_dir
    
def should_skip(f:str) -> bool:
    """
    * Indicate if should skip the current file.
    """
    # Skip anything not an Excel workbook:
    if not f.endswith(".xlsx"):
        return True
    # Skip the meta files:
    elif any(pt in f for pt in ["EIA-860 Form", "LayoutY"]):
        return True
    # Skip open files:
    elif f.startswith("~"):
        return True
    return False