'''
DATA INGESTION PIPELINE (MAIN)
------------------------------
this is the central orchestrator for all ingestion processes. 

this triggers specialized validation scripts for different data sources and handles the final upload to Supabase.

KEY NOTES:
- Data Flow: Models (Pydantic) -> Fetch (GSheets) -> Validate (Polars) -> Upload (Supabase).
- Orchestration: One failure won't kill the whole run; the script will catch errors per task and move to the next.
- Output Logs: Every run generates a brand new log file in the /logs directory.
- Statistics: Logging doesn't only show fail/success, but also shows description, count of rows, time intervals, and other workflow metrics 

'''

import sys
import os
import time
import config
from output_logging import setup_logging

log_file_path = setup_logging() # start logging before anything else

from ingestions.chart_of_accounts import get_validated_data as get_coa_data
from ingestions.expenses_02 import get_validated_data as get_expenses02_data
from ingestions.expenses_01 import get_validated_data as get_expenses01_data
from ingestions.invoices_01 import get_validated_data as get_invoices01_data
from ingestions.recurring_01 import get_validated_data as get_recurring01_data
from supabase_upload import upload_to_supabase

def run_task(name, fetch_func, table_name):

    start_time = time.time()
    
    try:
        df = fetch_func() 
        
        if not df.is_empty():
            row_count = len(df)
            upload_to_supabase(df, table_name)
            duration = round(time.time() - start_time, 2)
            
            print(f"STATUS: Processed {row_count} rows in {duration}s") # acts as a footer to each process block 
            return row_count
        else:
            print(f"STATUS: Skipped (No data found)")
            return 0
            
    except Exception as e:
        print(f"STATUS: ERROR - {str(e)}")
        return None

def main():
    width = 90
    total_start = time.time()
    
    # workflow started header
    print(f"\n{'-' * width}")
    print(f"{' DATA INGESTION STARTED '.center(width, '-')}")
    print(f"{'-' * width}")

    tasks = [
        ("Chart of Accounts", get_coa_data, 'chart_of_accounts'),
        ("Expenses 01", get_expenses01_data, 'latest_expenses_01'),
        ("Expenses 02", get_expenses02_data, 'latest_expenses_02'),
        ("Invoices 01", get_invoices01_data, 'latest_invoices_01'),
        ("Recurring Fees", get_recurring01_data, 'latest_recurring_fees_01')
    ]

    results = []
    for _, func, table in tasks:
        results.append(run_task(_, func, table))

    # workflow finished header
    print(f"\n{'-' * width}")
    print(f"{' DATA INGESTION FINISHED '.center(width, '-')}")
    print(f"{'-' * width}")

    # summary statistics
    total_duration = round(time.time() - total_start, 2)
    successful_tasks = [r for r in results if r is not None]
    total_rows = sum(filter(None, results))
    
    print(f"STATISTICS:")
    print(f"- Total Time: {total_duration}s")
    print(f"- Tasks: {len(successful_tasks)}/{len(tasks)} completed successfully")
    print(f"- Total Rows Processed: {total_rows}")

    # relative path for cleaner output
    relative_path = log_file_path.relative_to(config.shared_root) if 'shared_root' in dir(config) else log_file_path
    clean_path = str(relative_path).replace("\\", "/")

    print(f"\n{'='*width}")
    print(f" LOG SAVED TO: {clean_path} ".center(width, ' ')) # the first footer you saw was a sub footer, this one is a footer footer 
    print(f"{'='*width}\n")

if __name__ == '__main__':
    main()