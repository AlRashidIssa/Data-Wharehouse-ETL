import sys
import os
import pandas as pd
from pathlib import Path

# Define MAIN_DIR to point to the project root directory
MAIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(MAIN_DIR)

from utils import ErrorTrack, PipelineTrack
from databaseOperations.ingest_from_drive import LoadFromDrive
from databaseOperations.unzip_database import UnzipFile
from databaseOperations.extract_database import SQLiteExtractor
from databaseOperations.transform_database import TransformData
from analysis.load_from_csv import CSVLoader
from analysis.understandDataset import DataSetAnalyzer
from analysis.visualize_dataset import DataVisualizer


def etl_pipeline():
    """
    Main function to execute the ETL pipeline.
    """
    try:
        # Step 1: Load dataset from Google Drive
        PipelineTrack("Starting dataset ingestion from Google Drive...")
        drive_loader = LoadFromDrive()
        dataset_url = "https://drive.google.com/file/d/139OEjxiFwxJtFQWaixF0fG4JSQyGp6EP/view?usp=sharing"
        archive_dir = f"{MAIN_DIR}/data/archive"
        dataset_name = "train_data"
        drive_loader.load(url=dataset_url, save_archive=archive_dir, name=dataset_name)
        PipelineTrack("Dataset successfully downloaded.")

        # Step 2: Unzip the dataset
        PipelineTrack("Unzipping dataset...")
        unzipper = UnzipFile()
        zip_path = f"{archive_dir}/{dataset_name}.zip"
        extracted_dir = f"{MAIN_DIR}/data/extracted"
        unzipper.extract(zip_path=zip_path, extract_to=extracted_dir)
        PipelineTrack("Dataset successfully unzipped.")

        # Step 3: Extract data from SQLite database
        PipelineTrack("Extracting data from SQLite database...")
        db_path = f"{extracted_dir}/database.sqlite"
        query = "SELECT * FROM otp"
        extractor = SQLiteExtractor()
        extracted_data = extractor.extract(database=db_path, query=query)
        PipelineTrack("Data extraction completed. Rows fetched: {}".format(len(extracted_data)))

        # Step 4: Transform the data
        PipelineTrack("Transforming data...")
        transformer = TransformData()
        transformed_data = transformer.transform(extracted_data)
        PipelineTrack("Data transformation completed.")

        # Step 5: Load data from CSV (if needed for additional analysis)
        PipelineTrack("Loading data from CSV for analysis...")
        csv_loader = CSVLoader()
        csv_path = f"{MAIN_DIR}/data/csv/train_data.csv"  # Example CSV path
        csv_data = csv_loader.load(csv_path)
        PipelineTrack("CSV data loaded successfully.")

        # Step 6: Analyze the dataset
        PipelineTrack("Analyzing the dataset...")
        analyzer = DataSetAnalyzer()
        analysis_report = analyzer.analyze(transformed_data)
        analysis_report_path = f"{MAIN_DIR}/reports/analysis_report.csv"
        analysis_report.to_csv(analysis_report_path, index=False)
        PipelineTrack(f"Dataset analysis report saved at: {analysis_report_path}")

        # Step 7: Visualize the dataset
        PipelineTrack("Visualizing the dataset...")
        visualizer = DataVisualizer(save_dir=f"{MAIN_DIR}/reports/visualizations")
        visualizations_report = visualizer.visualize_and_save(transformed_data)
        visualizations_report_path = f"{MAIN_DIR}/reports/visualizations_report.csv"
        visualizations_report.to_csv(visualizations_report_path, index=False)
        PipelineTrack(f"Visualizations report saved at: {visualizations_report_path}")

        PipelineTrack("ETL pipeline execution completed successfully.")

    except Exception as e:
        error_msg = f"Error during ETL pipeline execution: {str(e)}"
        ErrorTrack(error_msg)
        raise Exception(error_msg) from e


if __name__ == "__main__":
    etl_pipeline()
