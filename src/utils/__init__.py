import logging
import os

# Define MAIN_DIR to point to the logs directory at the project root
MAIN_DIR = "/workspaces/Data-Wharehouse-ETL/logs"

print(f"Main Directory: {MAIN_DIR}")
# Create the logs directory if it doesn't exist
os.makedirs(MAIN_DIR, exist_ok=True)


def setup_logging(log_path: str, logger_name: str, logging_level: int):
    """
    Sets up logging for a specific logger and log file.

    Args:
        log_path (str): Path to the log file.
        logger_name (str): Unique name for the logger.
        logging_level (int): The logging level (e.g., logging.DEBUG, logging.INFO).
    """
    # Ensure the log directory exists
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # Create a logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging_level)  # Set the desired logging level

    # Check if handlers already exist to avoid duplication
    if not logger.hasHandlers():
        # Create file handler for logger
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)

        # Optionally, add console output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)

    return logger

# Log file paths for different components
path_pipeline = f"{MAIN_DIR}/PipelineOperation.log"
path_error = f"{MAIN_DIR}/ErrorTrack.log"

# Setup separate loggers for each component with appropriate logging levels
pipelineTrack = setup_logging(path_pipeline, "Pipeline:Track", logging.INFO)
errorTrack = setup_logging(path_error, "Error:Track", logging.ERROR)


# Logging functions for each component
def PipelineTrack(message: str):
    pipelineTrack.info(message)

def ErrorTrack(message: str):
    errorTrack.error(message)

# Test Case 
if __name__ == "__main__":
    PipelineTrack("Test Pipleine Track Monitor")
    ErrorTrack("Test Error Track Monitor")
