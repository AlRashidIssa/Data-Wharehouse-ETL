import sys, os
import pandas as pd
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
from analysis.visualize_dataset import TrainVisualization
from config import *

def etl_pipeline():
    """
    Main function to execute the ETL pipeline.
    """
    try:
        # Step 1: Load dataset from Google Drive
        PipelineTrack("Starting dataset ingestion from Google Drive...")
        drive_loader = LoadFromDrive()
        drive_loader.load(url=DATASETURL, save_archive=ARCHIVEDIR, name=DATABASENAME)
        PipelineTrack("Dataset successfully downloaded.")

        # Step 2: Unzip the dataset
        PipelineTrack("Unzipping dataset...")
        unzipper = UnzipFile()
        zip_path = os.path.join(ARCHIVEDIR, f"{DATABASENAME}.zip")
        unzipper.unzip(zip_path=zip_path, extract_to=EXTRACTEDDIR)
        PipelineTrack("Dataset successfully unzipped.")

        # Step 3: Extract data from SQLite database
        PipelineTrack("Extracting data from SQLite database...")
        db_path = os.path.join(EXTRACTEDDIR, f"{DATABASENAME}.sqlite")
        extractor = SQLiteExtractor()
        extractor.connect(db_path=db_path)
        EXTRACTEDDATA = extractor.execute_query(query=QUERY)
        EXTRACTEDDATA.to_csv(f"{CSVDATA}/csv_from_sql.csv")
        PipelineTrack(f"Data extraction completed. Rows fetched: {len(EXTRACTEDDATA)}")

        # Step 4: Transform the data
        PipelineTrack("Transforming data...")
        transformer = TransformData()
        TRANSFORMEDDATA = transformer.transform(df=EXTRACTEDDATA, df_wheresave=DATAWHARESAVE)
        PipelineTrack("Data transformation completed.")

        # Step 5: Load data from CSV (if needed for additional analysis)
        PipelineTrack("Loading data from CSV for analysis...")
        csv_loader = CSVLoader()
        csv_data = csv_loader.load_csv(file_path=f"{CSVDATA}csv_from_sql.csv")
        PipelineTrack("CSV data loaded successfully.")

        # Step 6: Analyze the dataset
        PipelineTrack("Analyzing the dataset...")
        analyzer = DataSetAnalyzer()
        analysis_report = analyzer.analyze(csv_data)
        analysis_report = pd.DataFrame(analysis_report)
        analysis_report.to_csv(f"{VISUALIZEOUTPUTDIR}/REPORT.csv")
        PipelineTrack(f"Dataset analysis report saved at: {DATAWHARESAVE}")

        # Step 7: Visualize the dataset
        visualizer = TrainVisualization(avg_delay_file=AVGDELAYFILE,
                                        train_status_file=TRAINSTATUSFILES, 
                                        output_dir=VISUALIZEOUTPUTDIR)
        PipelineTrack("Train Visualization Pipeline")
        visualizer.load_data()
        visualizer.process_data()
        visualizer.create_plots()
        PipelineTrack("Train Visualization Pipeline")

        PipelineTrack("ETL pipeline execution completed successfully.")

    except Exception as e:
        error_msg = f"Error during ETL pipeline execution: {str(e)}"
        ErrorTrack(error_msg)
        raise Exception(error_msg) from e


if __name__ == "__main__":
    etl_pipeline()
