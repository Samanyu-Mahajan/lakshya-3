import logging, datetime
import os

from config import BASE_LOG_PATH
formatter = logging.Formatter('%(asctime)s %(levelname)s, %(message)s')
trade_formatter = logging.Formatter('%(asctime)s Trade %(message)s')
base_path_logger = f"{BASE_LOG_PATH}"


def setup_logger(lock, name, log_file, log_formatter=formatter, level=logging.INFO):
    logger = logging.getLogger(name)

    handler = logging.FileHandler(log_file, mode='a')
    handler.setFormatter(log_formatter)

    logger.setLevel(level)
    original_emit = handler.emit
    def emit_with_lock(record):
        lock.acquire()
        try:
            original_emit(record)
        finally:
            lock.release()
    handler.emit = emit_with_lock



    logger.addHandler(handler)
    return logger


# General logger

# current_log_path = None
# general_logger = None


def setup_general_logger(lock, start_date):
    global current_log_path, general_logger
    # 
    current_log_path = f"{base_path_logger}/{start_date}"#quantx/logs/20250205/
    
    try:
        pass
        # print("doing deleting")
        # os.system(f"rm -rf {current_log_path}")  # delete existing log
        # os.mkdir(current_log_path)  # regenerate new log
    except:
        print(f"BASE path already exists {current_log_path}")
    base_log_path = f"{current_log_path}/stdout.log"
    # quantx/logs/20250205/stdout.log
    # set up for only start_date
    general_logger = setup_logger(lock, f'general_logger_{start_date}', f"{base_log_path}", formatter)
    return general_logger

# quantx/logs/20250205/stdout.log
def get_general_logger():
    return general_logger

#quantx/logs/20250205/
def get_current_log_path():
    return current_log_path

