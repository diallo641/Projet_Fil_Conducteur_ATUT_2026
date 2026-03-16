# etl_monitoring.py
import logging
import json
import time
from functools import wraps

# -------------------------
# Logging structuré JSON
# -------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module
        }
        return json.dumps(log_record)

def get_logger(name="etl_logger"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

logger = get_logger()

# -------------------------
# Decorateur pour mesurer la performance
# -------------------------
def monitor_performance(step_name):
    """
    Decorator pour mesurer le temps d'execution d'une fonction et logger en JSON
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(json.dumps({
                "step": step_name,
                "duration_sec": duration
            }))
            return result
        return wrapper
    return decorator
