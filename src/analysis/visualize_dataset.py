import os
import sys
from abc import ABC, abstractmethod
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Define MAIN_DIR to point to the project root directory
MAIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(MAIN_DIR)

from utils import ErrorTrack, PipelineTrack

class VisualizationBase(ABC):
    """
    Abstract Base Class for visualization. Defines the interface for processing and visualizing data.
    """

    @abstractmethod
    def __init__(self, avg_delay_file: str, train_status_file: str, output_dir: str):
        self.avg_delay_file = avg_delay_file
        self.train_status_file = train_status_file
        self.output_dir = output_dir
        self.avg_delay_df = None
        self.train_status_df = None
    @abstractmethod
    def load_data(self):
        """Load CSV data."""
        pass

    @abstractmethod
    def process_data(self):
        """Process and clean data."""
        pass

    @abstractmethod
    def create_plots(self):
        """Create visualizations."""
        pass

    def save_plot(self, plt_obj, filename):
        """Save the plot to the output directory."""
        filepath = os.path.join(self.output_dir, filename)
        plt_obj.savefig(filepath)
        plt_obj.close()

class TrainVisualization(VisualizationBase):
    """
    Implementation of VisualizationBase for train delay data visualization.
    """

    def __init__(self, avg_delay_file: str, train_status_file: str, output_dir: str):
        super().__init__(avg_delay_file, train_status_file, output_dir)

    def load_data(self):
        """Load CSV files into dataframes."""
        try:
            self.avg_delay_df = pd.read_csv(self.avg_delay_file)
            self.train_status_df = pd.read_csv(self.train_status_file)
        except Exception as e:
            ErrorTrack(e)
            raise

    def process_data(self):
        """Clean and prepare data for visualization."""
        try:
            self.train_status_df['timeStamp'] = pd.to_datetime(self.train_status_df['timeStamp'])
            self.train_status_df['date'] = pd.to_datetime(self.train_status_df['date'])
            self.train_status_df['hour'] = self.train_status_df['timeStamp'].dt.hour
        except Exception as e:
            ErrorTrack(e)
            raise

    def create_plots(self):
        """Create and save visualizations."""
        try:
            # Visualization 1: Average Delay per Train ID
            plt.figure(figsize=(12, 6))
            sns.barplot(data=self.avg_delay_df, x='train_id', y='avg_delay_minutes', palette='viridis')
            plt.title('Average Delay per Train ID')
            plt.ylabel('Average Delay (minutes)')
            plt.xlabel('Train ID')
            self.save_plot(plt, "average_delay_per_train_id.png")

            # Visualization 2: Delays over time
            plt.figure(figsize=(14, 7))
            sns.lineplot(data=self.train_status_df, x='timeStamp', y='delay_minutes', hue='direction', ci=None)
            plt.title('Train Delays Over Time')
            plt.ylabel('Delay (minutes)')
            plt.xlabel('Timestamp')
            self.save_plot(plt, "delays_over_time.png")

            # Visualization 3: Delay distribution by day of the week
            plt.figure(figsize=(10, 6))
            sns.boxplot(data=self.train_status_df, x='day_of_week', y='delay_minutes', palette='muted')
            plt.title('Delay Distribution by Day of the Week')
            plt.ylabel('Delay (minutes)')
            plt.xlabel('Day of the Week')
            self.save_plot(plt, "delay_distribution_by_day.png")

            # Visualization 4: Heatmap of Delays Throughout the Day
            delay_heatmap = self.train_status_df.pivot_table(
                index='day_of_week', columns='hour', values='delay_minutes', aggfunc='mean'
            )
            plt.figure(figsize=(14, 7))
            sns.heatmap(delay_heatmap, cmap='YlGnBu', annot=True, fmt=".1f")
            plt.title('Average Delay (Minutes) Heatmap by Hour and Day')
            plt.ylabel('Day of the Week')
            plt.xlabel('Hour of the Day')
            self.save_plot(plt, "heatmap_delays_by_hour_day.png")

            # Visualization 5: Delays by Origin Station
            station_delays = self.train_status_df.groupby('originStation')['delay_minutes'].sum().reset_index()
            plt.figure(figsize=(14, 6))
            sns.barplot(data=station_delays, x='originStation', y='delay_minutes', palette='viridis')
            plt.title('Total Delays by Origin Station')
            plt.ylabel('Total Delay (minutes)')
            plt.xlabel('Origin Station')
            plt.xticks(rotation=45)
            self.save_plot(plt, "delays_by_origin_station.png")

            # Visualization 6: Delays by Next Station
            next_station_delays = self.train_status_df.groupby('nextStation')['delay_minutes'].mean().reset_index()
            plt.figure(figsize=(14, 6))
            sns.barplot(data=next_station_delays, x='nextStation', y='delay_minutes', palette='cool')
            plt.title('Average Delays by Next Station')
            plt.ylabel('Average Delay (minutes)')
            plt.xlabel('Next Station')
            plt.xticks(rotation=45)
            self.save_plot(plt, "delays_by_next_station.png")

            # Visualization 7: Delay Distribution
            plt.figure(figsize=(10, 6))
            sns.histplot(data=self.train_status_df, x='delay_minutes', kde=True, bins=20, color='blue')
            plt.title('Distribution of Delays')
            plt.xlabel('Delay (minutes)')
            plt.ylabel('Frequency')
            self.save_plot(plt, "delay_distribution.png")

            # Visualization 8: Correlation Matrix (Numerical Columns Only)
            plt.figure(figsize=(10, 6))

            # Select only numerical columns
            numeric_columns = self.train_status_df.select_dtypes(include=['number'])
            correlation_matrix = numeric_columns.corr()

            # Plot the correlation matrix
            sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', vmin=-1, vmax=1)
            plt.title('Correlation Matrix of Numerical Columns in Train Data')
            self.save_plot(plt, "correlation_matrix.png")

        except Exception as e:
            ErrorTrack(e)
            raise




if __name__ == "__main__":
    try:
        # Define file paths (update these as per your environment)
        avg_delay_file = "/workspaces/Data-Wharehouse-ETL/database/clearnsave/delay_summary.csv"
        train_status_file = "/workspaces/Data-Wharehouse-ETL/database/clearnsave/df.csv"

        # Initialize and execute visualization pipeline
        visualizer = TrainVisualization(avg_delay_file=avg_delay_file,
                                        train_status_file=train_status_file,
                                        output_dir="/workspaces/Data-Wharehouse-ETL/visualize")
        PipelineTrack("Train Visualization Pipeline")
        visualizer.load_data()
        visualizer.process_data()
        visualizer.create_plots()
        PipelineTrack("Train Visualization Pipeline")
    except Exception as e:
        ErrorTrack(f"Pipeline execution failed: {str(e)}")
