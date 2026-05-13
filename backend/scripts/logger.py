# scripts/logger.py
import logging
import sys
import traceback

def setup_logger(name: str = __name__):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Console handler (stdout)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(ch)

    return logger

def log_state_and_exit(logger, error_msg, exit_code=1):
    """Log an error with full traceback, include useful system state."""
    logger.error(error_msg)
    logger.error(traceback.format_exc())
    logger.debug(f"Python version: {sys.version}")
    logger.debug(f"Platform: {sys.platform}")
    logger.debug(f"Command line: {sys.argv}")
    sys.exit(exit_code)