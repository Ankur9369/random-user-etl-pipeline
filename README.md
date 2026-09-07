
# Random User ETL Pipeline

A Python ETL pipeline that extracts user data from the [Random User API](https://randomuser.me/), transforms and validates it with pandas, and loads the results into CSV, JSON, and SQLite formats.

Repository: https://github.com/Ankur9369/random-user-etl-pipeline

## Features

- Extracts 5 batches of up to 1,000 users.
- Retries failed API requests.
- Flattens nested JSON responses.
- Removes duplicate email addresses.
- Adds full names, age groups, extraction timestamps, and pipeline versions.
- Validates required columns, email values, duplicate emails, and age ranges.
- Saves processed data to CSV, JSON, a backup CSV, and SQLite.
- Creates a JSON execution report and timestamped log file.

## Requirements

- Python 3.9 or newer
- Internet access for the Random User API

Install the Python dependencies:

```bash
pip install pandas requests
```

## Run the Pipeline

From this directory, run:

```bash
python etl_pipline_complete.py
```

The script returns exit code `0` when the pipeline completes successfully and exit code `1` when it fails.

## Pipeline Flow

1. **Extract**: Requests user data from the Random User API in batches.
2. **Transform**: Flattens the nested response, selects useful fields, cleans values, and creates derived columns.
3. **Validate**: Checks that the data is present, columns exist, emails are valid and unique, and ages are within range.
4. **Load**: Writes the processed data to output files and SQLite.
5. **Report**: Saves processing metrics as a JSON report.

## Generated Files

| Location | Description |
| --- | --- |
| `output/users_*.csv` | Processed users in CSV format |
| `output/users_*.json` | Processed users in JSON format |
| `backups/users_backup_*.csv` | Timestamped CSV backup |
| `etl_database.db` | SQLite database containing the `users` table |
| `reports/report_*.json` | Pipeline performance and data summary |
| `logs/etl_*.log` | Execution logs |

Generated files are ignored by Git and should not be committed.

## Configuration

Pipeline settings are defined in the `Settings` dataclass inside `etl_pipline_complete.py`, including:

- API URL
- Batch size and number of batches
- Request timeout and retry settings
- Output, backup, report, and database paths

## Project Structure

```text
etl-project/
├── etl_pipline_complete.py  # Main ETL pipeline
├── etl_project.py            # Simple weather ETL example
├── backups/                  # Generated backup files
├── output/                   # Generated CSV and JSON files
├── reports/                  # Generated reports
├── logs/                     # Generated log files
└── etl_database.db          # Generated SQLite database
```

