import logging
import sys
from datetime import datetime

def setup_logger(name, log_level=logging.INFO):
    """
    Set up a logger with consistent formatting and handlers
    
    Args:
        name (str): Name of the logger
        log_level (int): Logging level (default: INFO)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(detailed_formatter)
    logger.addHandler(console_handler)
    
    # Create file handler
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_handler = logging.FileHandler(f'logs/{name}_{timestamp}.log')
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    return logger

def log_error(logger, error, context=None):
    """
    Log an error with context information
    
    Args:
        logger (logging.Logger): Logger instance
        error (Exception): The error to log
        context (dict, optional): Additional context information
    """
    error_msg = f"Error: {str(error)}"
    if context:
        error_msg += f" | Context: {context}"
    logger.error(error_msg, exc_info=True)

def log_info(logger, message, context=None):
    """
    Log an informational message with context
    
    Args:
        logger (logging.Logger): Logger instance
        message (str): The message to log
        context (dict, optional): Additional context information
    """
    if context:
        message = f"{message} | Context: {context}"
    logger.info(message) 