import logging
from datetime import datetime
import os

def setup_logging(log_filename):
    """
    Set up logging configuration.
    Logs will be stored in the 'logs' directory with a timestamped filename.
    """
    # Ensure the logs directory exists
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Configure logging
    logging.basicConfig(
        filename=log_filename,
        level=logging.ERROR,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )

def log_error(error_message, exc_info=True):
    """
    Log an error message with optional exception info.
    
    Args:
        error_message (str): The error message to log.
        exc_info (bool): Whether to include exception info in the log.
    """
    logging.error(error_message, exc_info=exc_info)
