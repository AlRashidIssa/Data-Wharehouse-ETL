import sqlite3, sys, os
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod

# Define MAIN_DIR to point to the project root directory
MAIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(MAIN_DIR)
from utils import ErrorTrack, PipelineTrack

class ICSVLoader(ABC):
    """
    Abstract Base Class for loading CSV files as a Pandas DataFrame.
    """

    @abstractmethod
    def load_csv(self, file_path: str, delimiter: str = ",") -> pd.DataFrame:
        """
        Load a CSV file into a Pandas DataFrame.

        Parameters:
        -----------
        file_path (str): Path to the CSV file.
        delimiter (str): Delimiter used in the CSV file (default: ',').

        Returns:
        --------
        pd.DataFrame: Loaded DataFrame.

        Raises:
        -------
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or improperly formatted.
        TypeError: If `file_path` is not a string.
        """
        pass

class CSVLoader(ICSVLoader):
    """
    Concrete implementation of ICSVLoader for loading CSV files.
    """

    def load_csv(self, file_path: str, delimiter: str = ",") -> pd.DataFrame:
        """
        Load a CSV file into a Pandas DataFrame with error handling and logging.

        Parameters:
        -----------
        file_path (str): Path to the CSV file.
        delimiter (str): Delimiter used in the CSV file (default: ',').

        Returns:
        --------
        pd.DataFrame: Loaded DataFrame.
        """
        try:
            # Validate input type
            if not isinstance(file_path, str):
                error_msg = f"The file path must be a string. Provided: {type(file_path)}"
                ErrorTrack(error_msg)
                raise TypeError(error_msg)

            # Check if file exists
            file_path_obj = Path(file_path)
            if not file_path_obj.exists() or not file_path_obj.is_file():
                error_msg = f"CSV file not found: {file_path}"
                ErrorTrack(error_msg)
                raise FileNotFoundError(error_msg)

            # Load the CSV into a DataFrame
            df = pd.read_csv(file_path, delimiter=delimiter)
            if df.empty:
                error_msg = f"The CSV file at {file_path} is empty."
                ErrorTrack(error_msg)
                raise ValueError(error_msg)

            # Log successful loading
            PipelineTrack(f"CSV file loaded successfully. Rows fetched: {len(df)}")
            return df

        except Exception as e:
            error_msg = f"Error while loading CSV file: {str(e)}"
            ErrorTrack(error_msg)
            raise Exception(error_msg) from e


# Usage Example
if __name__ == "__main__":
    # Define the path to your CSV file
    delay_summary_path = "/workspaces/Data-Wharehouse-ETL/database/clearnsave/df.csv"
    df_path = "/workspaces/Data-Wharehouse-ETL/database/clearnsave/df.csv"


    # Instantiate the CSV loader
    csv_loader = CSVLoader()

    try:
        # Load the CSV file
        delay_summary = csv_loader.load_csv(file_path=delay_summary_path)
        df = csv_loader.load_csv(file_path=df_path)


        # Display the DataFrame
        print("Loaded DataFrame:")
        print(df.head())

    except Exception as e:
        print(f"Failed to load CSV: {str(e)}")