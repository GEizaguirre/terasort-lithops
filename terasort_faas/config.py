import os

# IO
MAX_RETRIES: int = 8
MAX_READ_TIME: int = 60
RETRY_WAIT_TIME: float = 0.5
OUTPUT_PREFIX="out"

# LOGGING
CONSOLE_LOGGER = "console_logger"
EXECUTION_LOGGER = "execution_logger"
HOME_PATH = os.path.expanduser('~')
LOG_PATH=HOME_PATH+"/.terasort-lithops/"

# UI
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'