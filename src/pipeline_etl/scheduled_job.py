import schedule, os, sys
import time
# Define MAIN_DIR to point to the project root directory
MAIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(MAIN_DIR)
from pipeline_etl.run import etl_pipeline
def run_etl():
    # Call your ETL pipeline function here
    etl_pipeline()

# Schedule the ETL to run every 2 hours
schedule.every(10).minutes.do(run_etl)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
