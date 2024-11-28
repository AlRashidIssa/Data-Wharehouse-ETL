import sqlite3, sys, os
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod

# Define MAIN_DIR to point to the project root directory
MAIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(MAIN_DIR)
from utils import ErrorTrack, PipelineTrack

class IDatabaseExtractor(ABC):
    """
    Abstract Base Class (ABC) for extracting data from a database.
    """

    @abstractmethod
    def connect(self, db_path: str) -> None:
        """
        Connect to the SQLite database.

        Parameters:
        -----------
        db_path (str): Path to the SQLite database file.

        Returns:
        --------
        None
        """
        pass

    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a query on the database and return the results as a DataFrame.

        Parameters:
        -----------
        query (str): SQL query to execute.

        Returns:
        --------
        pd.DataFrame: Results of the query as a pandas DataFrame.
        """
        pass

    @abstractmethod
    def close_connection(self) -> None:
        """
        Close the database connection.

        Returns:
        --------
        None
        """
        pass


class SQLiteExtractor(IDatabaseExtractor):
    """
    Concreate implementation fo IDatabaseExtractor for SQLite database.
    """
    def __init__(self) -> None:
        self.connection = None
    
    def connect(self, db_path: str) -> None:
        """
        Connect to the SQLite database.

        Parameters:
        ___________
        db_path (str): Path to the SQLite database file.

        Raises:
        _______
        FileNotFoundError: If the database file does not exist.
        TypeError: If the database Pth is not String
        sqlite3.Error: For any SQLite-specific connection errors.

        Returns:
        ________
        None
        """
        if not isinstance(db_path, str):
            error_msg = f"The database path must be a string. Provided type: {type(db_path)}"
            ErrorTrack(error_msg)
            raise TypeError(error_msg)
        
        if not Path(db_path).exists():
            error_msg = f"The database file does not exist: {db_path}"
            ErrorTrack(error_msg)
            raise FileNotFoundError(error_msg)
        try:
            self.connection = sqlite3.connect(db_path)
            PipelineTrack(f"Connected to database: {db_path}")
        except sqlite3.Error as e:
            error_msg = f"Error connecting to database: {str(e)}"
            ErrorTrack(error_msg)
            raise sqlite3.Error(error_msg)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a query on the database and return the results as a DataFrame.

        Parameters:
        -----------
        query (str): SQL query to execute.

        Raises:
        _______
        ValuerErro: If  the query is not a valid string.
        sqlite.Error: If an error occurs during query execution.
        Exception: For any other unforeseen erros.

        Returns:
        ________
        pd.DataFrame: Results of the query as a pandas DataFrame.
        """
        if not isinstance(query, str):
            error_msg = f"The query must be a string. Provided type: {type(query)}"
            ErrorTrack(error_msg)
            raise TypeError(error_msg)
        
        if not query.strip():
            error_msg = "The query strin is empty or invalid."
            ErrorTrack(error_msg)
            raise ValueError(error_msg)
        
        try:
            PipelineTrack(f"Executing query: {query}")
            df = pd.read_sql_query(query, self.connection)
            PipelineTrack(f"Query executed successfully. Rows fetched: {len(df)}")
            return df
        except sqlite3.Error as e:
            error_msg = f"Error executing query: {str(e)}"
            ErrorTrack(error_msg)
            raise sqlite3.Error(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during query execution: {str(e)}"
            ErrorTrack(error_msg)
            raise Exception(error_msg)

    def close_connection(self) -> None:
        """
        Close the database connection.

        Returns:
        --------
        None
        """
        try:
            if self.connection:
                self.connection.close()
                PipelineTrack("Database connection closed.")
        except sqlite3.Error as e:
            error_msg = f"Error closing the database connection: {str(e)}"
            ErrorTrack(error_msg)
            raise sqlite3.Error(error_msg)


if __name__ == "__main__":
    # Instantiate the extractor
    db_extractor = SQLiteExtractor()

    # Define the database file path
    database_path = "/workspaces/Data-Wharehouse-ETL/database/sql/database.sqlite"

    # Define the SQL query
    sql_query = "SELECT * FROM otp"

    try:
        # Connect to the database
        db_extractor.connect(database_path)

        # Execute the query and fetch results as DataFrame
        data_frame = db_extractor.execute_query(sql_query)
        print(data_frame.head())

        # Close the connection
        db_extractor.close_connection()
    except Exception as e:
        print(f"Error: {e}")
    