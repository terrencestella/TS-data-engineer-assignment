import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

def setup_logging():
    """
    Set up a logger named "DailyLogger" with a daily rotating file handler.
    
    The logger is set to INFO level by default, and the log messages are written 
    to a file named 'daily_log_<current_date>.log', which rotates at midnight every day.
    Up to 30 days of log files are retained.

    :return: The configured logger.
    :rtype: logging.Logger
    """
    # Create a logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)  # Set logging level to INFO or DEBUG as per your need

    # Create a handler that writes log messages to a file, with a daily log rotation
    handler = TimedRotatingFileHandler('logs\daily_log_{}.log'.format(datetime.now().strftime('%Y_%m_%d')), when="midnight", interval=1, backupCount=30)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger

logger = setup_logging()