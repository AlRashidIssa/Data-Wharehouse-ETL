import sys, os
import pandas as pd
from abc import ABC, abstractmethod

# Define MAIN_DIR to point to the project root directory
MAIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(MAIN_DIR)
from utils import ErrorTrack, PipelineTrack

r"""
Possible Transformations:
Filter Rows: Remove unnecessary rows, e.g., records with status as On Time.
Convert Data Types: Ensure date and timeStamp are in proper datetime format for analysis.
Add New Columns:
Calculate travel delays (delay_minutes).
Add day of the week based on date.
Rename Columns: Standardize column names (e.g., next_station → nextStation).
Aggregate Data: Group data by train_id and compute summary statistics like average delay.
Drop Unnecessary Columns: Remove columns that don’t contribute to downstream processing.
"""

class ITransformData(ABC):
    """
    Abstract Base Class (ABC) for transforming data.
    """

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the DataFrame.

        Parameters:
        -----------
        df (pd.DataFrame): The input DataFrame.

        Returns:
        --------
        pd.DataFrame: The transformed DataFrame.
        """
        pass

class TransformData(ITransformData):
    """
    Concrete implementation of ITransformData for transforming ETL data.
    """

    def transform(self, df: pd.DataFrame, df_wheresave: str) -> pd.DataFrame:
        """
        Transform the input DataFrame.

        Parameters:
        -----------
        df (pd.DataFrame): The input DataFrame.
        df_wheresave (str): The path for save dataframe after clearing

        Returns:
        --------
        pd.DataFrame: The transformed DataFrame.
        """
        try:
            # Log initial transformation start
            PipelineTrack("Starting data transformation.")

            # 1. Remove rows with 'On Time' status
            df = df[df['status'] != 'On Time']
            PipelineTrack(f"Filtered 'On Time' rows. Remaining rows: {len(df)}")

            # 2. Convert 'date' and 'timeStamp' to datetime
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
            df['timeStamp'] = pd.to_datetime(df['timeStamp'])
            PipelineTrack("Converted 'date' and 'timeStamp' to datetime format.")

            # 3. Add new columns
            # Example: Calculate delays (convert 'status' like '1 min' to integer delay)
            df['delay_minutes'] = df['status'].str.extract(r'(\d+)').astype(float)
            df['day_of_week'] = df['date'].dt.day_name()
            PipelineTrack("Added 'delay_minutes' and 'day_of_week' columns.")

            # 4. Rename columns for consistency
            df.rename(columns={
                'next_station': 'nextStation',
                'origin': 'originStation'
            }, inplace=True)
            PipelineTrack("Renamed columns for consistency.")

            # 5. Aggregate Data 
            delay_summary = df.groupby('train_id')['delay_minutes'].mean().reset_index()
            delay_summary.rename(columns={'delay_minutes': 'avg_delay_minutes'}, inplace=True)
            PipelineTrack("Aggregated data to calculate average delays by train_id.")

            # Log transformation completion
            PipelineTrack("Data transformation completed successfully.")
            df.to_csv(f"{df_wheresave}/df.csv")
            delay_summary.to_csv(f"{df_wheresave}/delay_summary.csv")
            return df, delay_summary

        except Exception as e:
            error_msg = f"Error during data transformation: {str(e)}"
            ErrorTrack(error_msg)
            raise Exception(error_msg) from e



if __name__ == "__main__":
    # Load data (simulate the earlier query result)
    data = {
        'train_id': [778, 598, 279, 476, 474],
        'direction': ['N', 'N', 'S', 'N', 'N'],
        'origin': ['Trenton', 'Thorndale', 'Elm', 'Airport Terminal E-F', 'Airport Terminal E-F'],
        'next_station': ['Stenton', 'Narberth', 'Ridley Park', 'Suburban Station', 'Jenkintown-Wyncote'],
        'date': ['2016-03-23'] * 5,
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

    # Instantiate transformer and transform data
    transformer = TransformData()
    transformed_df, summary_df = transformer.transform(df)

    # Display transformed data
    print("Transformed DataFrame:")
    print(transformed_df.head())

    print("\nDelay Summary:")
    print(summary_df.head())