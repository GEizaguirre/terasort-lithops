
from lithops.constants import LITHOPS_TEMP_DIR
from terasort_faas.config import *
import shutil
import os
import logging

def setup_logger(timestamp: str = ""):


    console_logger = logging.getLogger(CONSOLE_LOGGER)
    console_logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    console_formatter = logging.Formatter("%(asctime)s [PID:%(process)d] [%(levelname)s] %(filename)s:%(lineno)s -- %(message)s")
    console_handler.setFormatter(console_formatter)

    console_logger.addHandler(console_handler)


    execution_logger = logging.getLogger(EXECUTION_LOGGER)
    execution_logger.setLevel(logging.DEBUG)

    if not os.path.exists(LOG_PATH):
        console_logger.info(f"Creating directory for execution logs ({LOG_PATH})")
        os.makedirs(LOG_PATH)

    execution_handler = logging.FileHandler(os.path.join(LOG_PATH, "%s.yaml"%(timestamp)))
    execution_handler.setLevel(logging.DEBUG)

    execution_formatter = logging.Formatter("%(message)s")
    execution_handler.setFormatter(execution_formatter)

    execution_logger.addHandler(execution_handler)



def setup_lithops_logs(executor):

        if executor.backend == "localhost":
              runner_path = os.path.join(LITHOPS_TEMP_DIR, 'runner.py')
              shutil.copy("lithops_patches/runner.py", runner_path)