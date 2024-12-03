import yaml
import os
import sys
from typing import Dict, Any, Optional

from pathlib import Path

# Dynamically locate the main project directory
current_dir = Path(__file__).resolve().parent
MAIN_DIR = current_dir
while MAIN_DIR.name != "Data-Wharehouse-ETL":
    MAIN_DIR = MAIN_DIR.parent
# Get the absolute path to the directory one level above the current file's directory
print(MAIN_DIR)
CONFIGDIR = os.path.join(MAIN_DIR, "configs")
print(CONFIGDIR)  # Prints the absolute path to the 'configs' directory

sys.path.append(f"{MAIN_DIR}/src")

# Importing logging utilities (assuming they are implemented in 'utils')
from utils import ErrorTrack, PipelineTrack

def config_yaml_reader(file_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Reads a YAML configuration file and returns its contents as a dictionary.

    Args:
        file_path (str): Path to the YAML file.

    Returns:
        Dict[str, Any]: Parsed content of the YAML file.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        yaml.YAMLError: If there is an error while parsing the YAML file.
    """
    try:
        # If not, look for any YAML file in the specified directory
        config_files = os.listdir(CONFIGDIR)
        for con in config_files:
            if con.endswith(".yaml"):
                # Set the first YAML file found as the configuration file
                file_path = os.path.join(CONFIGDIR, con)
                PipelineTrack(f"Using alternative configuration file: {file_path}")
                break
            else:
                # If no YAML file is found, raise an error
                error_msg = "No YAML configuration file found in the directory."
                ErrorTrack(error_msg)
                raise FileNotFoundError(error_msg)

        # Load the YAML file
        with open(file_path, 'r') as f:
            config = yaml.safe_load(f)
            PipelineTrack("Successfully loaded configuration from YAML file.")
            return config
    except FileNotFoundError as e:
            ErrorTrack(f"File not found error: {e}")
            raise
    except yaml.YAMLError as e:
        ErrorTrack(f"Error parsing YAML file: {e}")
        raise ValueError(f"Error parsing YAML file: {e}")

configs = config_yaml_reader(None)
DATASETURL = configs["etl_config"]["dataset_url" ]
ARCHIVEDIR = configs["etl_config"]["archive_dir"]
DATABASENAME = configs["etl_config"]["database_name"]
EXTRACTEDDIR = configs["etl_config"]["extracted_dir"]
DBPATH =  configs["etl_config"]["db_path"]
QUERY = configs["etl_config"]["query"]
AVGDELAYFILE = configs["etl_config"]["avg_delay_file"]
TRAINSTATUSFILES = configs["etl_config"]["train_status_file"]
VISUALIZEOUTPUTDIR = configs["etl_config"]["visualize_output_dir"]
DATAWHARESAVE = configs["etl_config"]["data_wharesave"]
CSVDATA = configs["etl_config"]["csv_data"]