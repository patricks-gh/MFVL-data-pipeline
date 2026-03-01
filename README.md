# Raw Data Ingestion Pipeline

A robust, configuration-driven ETL pipeline designed to orchestrate the ingestion of raw financial data from Google Sheets into Supabase. Built with Polars for high-performance data manipulation, Pydantic for schema-strict validation, and **uv** for modern Python dependency management.

## The MFVL Engine
The pipeline follows a strict **Model → Fetch → Validate → Load** architectural pattern to ensure zero-data-loss and high integrity.

``` ini
  [ MODEL ]  ──▶  [ FETCH ]  ──▶  [ VALIDATE ]  ──▶  [ LOAD ]
      │             │                │               │
  Pydantic        gspread          Polars         Supabase
  Contracts       API Ops          Cleaning       Upsert Logic
``` 

* **MODEL:** Defines the "Source of Truth" via Pydantic Data Contracts.
* **FETCH:** Programmatically extracts raw data from Google Sheets via gspread.
* **VALIDATE:** Checks every row against the model; isolates errors without crashing the pipeline.
* **LOAD:** Performs atomic upserts into Supabase via a universal uploader.

## Technical Stack

* **Environment:** Python 3.12+
* **Package Manager:** [uv](https://github.com/astral-sh/uv)
* **Processing:** Polars (DataFrame library)
* **Validation:** Pydantic V2
* **Database:** Supabase (Postgres)
* **Timezone Standard:** America/Los_Angeles (PST)

## Project Structure
``` bash
rawdata-ingestion/
├── pyproject.toml          # Project metadata and uv dependency definitions
├── uv.lock                 # Deterministic lockfile for environment consistency
├── main.py                 # Central orchestrator and execution entry point
├── config.py               # Environment variables and global configurations
├── output_logging.py       # Dual-stream (Terminal + File) logging utility
├── supabase_upload.py      # Universal uploader with dynamic schema mapping
├── scripts/                # Task-specific execution logic
│   ├── models/             # Pydantic schema definitions (Data Contracts)
│   │   ├── base.py
│   │   ├── accounting.py
│   │   └── expenses.py
│   └── ingestions/         # Source-specific logic (Fetching + Polars)
│       ├── chart_of_accounts.py
│       ├── expenses_01.py
│       └── ... 
└── logs/                   # Local execution audit trails (Git Ignored)
    ├── main_sample.log     # Sanitized overview of full pipeline execution
    ├── expenses_sample.log # Examples of expense validation errors
    ├── invoice_sample.log  # Examples of invoice data contract enforcement
    └── recurring_sample.log # Examples of recurring fee null-value handling
```
## Setup and Execution

1.  **Environment Configuration:**
    Create a .env file in the root directory:
    
    # Supabase Configuration
    ``` ini
    SUPABASE_URL=your_project_url
    SUPABASE_KEY=your_service_role_key
    ```
    # Google Sheets Configuration
    ``` ini
    GOOGLE_SHEETS_CREDENTIALS_PATH=path/to/service_account.json
    GOOGLE_SHEET_ID=your_spreadsheet_id_from_url
    GSHEETS_TAB_NAMES=Tab1,Tab2,Tab3
    ```
    # Paths
    SHARED_ROOT=path_to_project_root

2.  **Google Sheets Permissions:**
    * Share your target Google Sheet with the service account email found in your credentials JSON.

3.  **Run Pipeline:**
    uv sync
    uv run main.py

## Data Integrity & Logging

The system implements a dual-stream redirection pattern, sending stdout and stderr to both the console and daily log files. 

### Live Log Preview (main.py):
#### `main_sample.log` (Overall Pipeline Execution)

``` ini
------------------------------------------------------------------------------------------
--------------------------------- DATA INGESTION STARTED ---------------------------------
------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
 PROCESS: [MODULE_NAME] 
------------------------------------------------------------------------------------------
Successfully connected to source: [DATA_SOURCE] | Tab: [TAB_NAME]
Validation complete. [N] rows cleared.
[N] rows available. Uploading to [SCHEMA].[TABLE_NAME]
SUCCESS: Uploaded to Supabase at [DATE] at [TIME] PST.
STATUS: Processed [N] rows in [X.XX]s

------------------------------------------------------------------------------------------
 PROCESS: [MODULE_NAME] 
------------------------------------------------------------------------------------------
Successfully connected to source: [DATA_SOURCE] | Tab: [TAB_NAME]
Validation complete. [N] rows cleared.
[N] rows available. Uploading to [SCHEMA].[TABLE_NAME]
SUCCESS: Uploaded to Supabase at [DATE] at [TIME] PST.
STATUS: Processed [N] rows in [X.XX]s

------------------------------------------------------------------------------------------
 PROCESS: [MODULE_NAME] 
------------------------------------------------------------------------------------------
Successfully connected to source: [DATA_SOURCE] | Tab: [TAB_NAME]
Validation complete. [N] rows cleared.
[N] rows available. Uploading to [SCHEMA].[TABLE_NAME]
SUCCESS: Uploaded to Supabase at [DATE] at [TIME] PST.
STATUS: Processed [N] rows in [X.XX]s

------------------------------------------------------------------------------------------
 PROCESS: [MODULE_NAME] 
------------------------------------------------------------------------------------------
Successfully connected to source: [DATA_SOURCE] | Tab: [TAB_NAME]
Validation complete. [N] rows cleared.
[N] rows available. Uploading to [SCHEMA].[TABLE_NAME]
SUCCESS: Uploaded to Supabase at [DATE] at [TIME] PST.
STATUS: Processed [N] rows in [X.XX]s

------------------------------------------------------------------------------------------
-------------------------------- DATA INGESTION FINISHED ---------------------------------
------------------------------------------------------------------------------------------
STATISTICS:
- Total Time: [XX.XX]s
- Tasks: [X]/[X] completed successfully
- Total Rows Processed: [NNN]

==========================================================================================
          LOG SAVED TO: [PROJECT_ROOT]/logs/[FILENAME].log          
==========================================================================================
```
## Operational Notes

* **Upsert Logic:** The pipeline uses on_conflict resolution. Existing records are updated based on their primary keys; new records are appended.
* **Auditing:** Every record is injected with a record_updated_at ISO timestamp in PST to track data freshness.
* **Performance:** High-performance data handling via Polars ensures minimal memory overhead during large-scale ingestion tasks.


