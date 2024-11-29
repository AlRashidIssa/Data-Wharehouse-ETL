import sqlite3, sys, os
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod

# Define MAIN_DIR to point to the project root directory
MAIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(MAIN_DIR)
from utils import ErrorTrack, PipelineTrack


class IDataSetAnalyzer(ABC):
    """
    Abstract Base Class for dataset analysis.
    """

    @abstractmethod
    def analyze(self, df: pd.DataFrame) -> dict:
        """
        Analyze the dataset and provide insights.

        Parameters:
        ___________
        df (pd.DataFrame): The input DataFrame.

        Returns:
        ________
        info (pd:DataFrame): Returns all the Information about dataset in DataFrame.
        """
        pass
class DataSetAnalyzer(IDataSetAnalyzer):
    """
    Concrete implementation of IDataSetAnalyzer for understanding a dataset.
    """

    def __init__(self) -> None:
        self.results = {}

    def analyze(self, df: pd.DataFrame) -> dict:
        """
        Perform a comprehensive analysis of the dataset.

        Parameters:
        -----------
        df (pd.DataFrame): The input DataFrame.

        Returns:
        --------
        dict: A dictionary containing analysis results.
        """
        try:
            # 1. Dataset Overview
            PipelineTrack("Gathering dataset overview...")
            self.results["Basic Info"] = str(df.info(verbose=True, buf=None))
            self.results["Shape"] = {"Rows": df.shape[0], "Columns": df.shape[1]}
            self.results["First Rows"] = df.head().to_dict()

            # 2. Missing Values
            PipelineTrack("Checking for missing values...")
            self.results["Missing Values"] = df.isnull().sum().to_dict()

            # 3. Descriptive Statistics
            PipelineTrack("Calculating descriptive statistics...")
            self.results["Descriptive Stats"] = df.describe(include='all').to_dict()

            # 4. Data Types and Memory Usage
            PipelineTrack("Analyzing data types and memory usage...")
            self.results["Data Types"] = df.dtypes.apply(str).to_dict()
            self.results["Memory Usage"] = df.memory_usage(deep=True).to_dict()

            # 5. Unique Values
            PipelineTrack("Counting unique values...")
            self.results["Unique Values"] = {col: df[col].nunique() for col in df.columns}

            # 6. Numeric Column Analysis
            numeric_columns = df.select_dtypes(include=['number']).columns
            self.results["Numeric Analysis"] = {
                col: {"Stats": df[col].describe().to_dict(), "Skewness": df[col].skew()}
                for col in numeric_columns
            }

            # 7. Categorical Column Analysis
            categorical_columns = df.select_dtypes(include=['object', 'category']).columns
            self.results["Categorical Analysis"] = {
                col: df[col].value_counts().to_dict() for col in categorical_columns
            }

            # 8. Duplicate Rows
            PipelineTrack("Checking for duplicates...")
            self.results["Duplicate Rows"] = {"Count": df.duplicated().sum()}

            # 9. Correlation Matrix
            if not numeric_columns.empty:
                PipelineTrack("Calculating correlation matrix...")
                self.results["Correlation Matrix"] = df[numeric_columns].corr().to_dict()

            return self.results

        except Exception as e:
            error_msg = f"Error during dataset analysis: {str(e)}"
            ErrorTrack(error_msg)
            raise Exception(error_msg) from e


# Usage Example
if __name__ == "__main__":
    # Simulate loading a sample dataset
    data = {
        'train_id': [778, 598, 279, 476, 474],
        'direction': ['N', 'N', 'S', 'N', 'N'],
        'origin': ['Trenton', 'Thorndale', 'Elm', 'Terminal E', 'Terminal F'],
        'next_station': ['Stenton', 'Narberth', 'Ridley Park', 'Suburban Station', 'Wyncote Park'],
        'date': ['2016-03-23', '2016-03-23', '2016-03-23', '2016-03-23', '2016-03-23'],
        'status': ['1 min', '1 min', '2 min', 'On Time', 'On Time'],
        'timeStamp': [
            '2016-03-23 00:01:47',
            '2016-03-23 00:01:58',
            '2016-03-23 00:02:02',
            '2016-03-23 00:03:19',
            '2016-03-23 00:03:35',
        ],
    }

    df = pd.DataFrame(data)

    # Instantiate dataset analyzer
    analyzer = DataSetAnalyzer()

    # Perform analysis
    result =  analyzer.analyze(df)
    result