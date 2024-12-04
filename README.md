# ETL Pipeline Project

## Overview
This project implements a fully functional **ETL pipeline** (Extract, Transform, Load) designed for local execution using Python and SQLite. The pipeline automates data retrieval, processing, and storage, preparing a clean dataset for downstream machine learning workflows.

## Features
1. **Extract**: Fetches data from an online API.
2. **Transform**: Cleans and processes raw data using Python and pandas.
3. **Load**: Stores the transformed data into a local SQLite database.
4. **Automation**: The pipeline runs automatically every 2 hours.

## Tech Stack
- **Programming Language**: Python
- **Libraries**: pandas, requests, sqlite3
- **Database**: SQLite
- **Scheduler**: Python `schedule` or OS-based scheduling tools (e.g., cron, Task Scheduler)

## Installation

### Prerequisites
- Python 3.8 or later
- SQLite (built into Python)

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/AlRashidIssa/Data-Wharehouse-ETL
   cd etl-pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up the SQLite database:
   ```python
   python setup_db.py
   ```

4. Update the `config.py` file with your API URL and database details.

## Usage

### Run Manually
Execute the pipeline manually:
```bash
python etl_pipeline.py
```

### Automate the Pipeline
#### Using Python's `schedule` Library
Run the scheduler script:
```bash
python scheduler.py
```

#### Using Cron (Linux/Mac)
Add the following entry to your crontab to run the pipeline every 2 hours:
```bash
0 */2 * * * python3 /path/to/etl_pipeline.py
```

#### Using Task Scheduler (Windows)
Set up a new task in Windows Task Scheduler to run `etl_pipeline.py` every 2 hours.

## Directory Structure
```
├── etl_pipeline.py      # Main ETL script
├── setup_db.py          # Script to initialize SQLite database
├── scheduler.py         # Script for scheduling the pipeline
├── config.py            # Configuration file (API URLs, DB paths)
├── requirements.txt     # Python dependencies
└── README.md            # Project documentation
```

## Challenges and Learnings
- **API Integration**: Handling edge cases and errors during data extraction.
- **Data Transformation**: Ensuring scalability and efficiency during processing.
- **Automation**: Balancing resource usage and data freshness.

## Future Improvements
- Add support for cloud-based databases (e.g., PostgreSQL, AWS RDS).
- Implement logging for better monitoring and debugging.
- Optimize data transformations for larger datasets.

## License
This project is for personal use only and not licensed for commercial distribution.