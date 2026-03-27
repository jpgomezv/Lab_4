import logging
import sys
import os

def setup_logger(name: str) -> logging.Logger:
    """Sets up a standardized logger for the ETL pipeline."""
    # Ensure logs directory exists if we decide to log to file
    os.makedirs('logs', exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers if setup_logger is called multiple times
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        # File handler (optional, but good practice)
        fh = logging.FileHandler('logs/pipeline.log')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
    return logger
