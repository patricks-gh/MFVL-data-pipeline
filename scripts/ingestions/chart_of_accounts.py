import gspread
import polars as pl
from pathlib import Path
from pydantic import ValidationError
import config
from models.chart_of_accounts import ChartOfAccountsRow

# creates a visual break in the terminal logs
def print_divider(title: str):
    print(f"\n{'-'*90}")
    print(f" PROCESS: {title.upper()} ")
    print(f"{'-'*90}")

# handles the google sheets connection and returns the specific worksheet
def get_worksheet() -> gspread.Worksheet:
    creds_path = config.shared_root / config.GOOGLE_SERVICE_ACCOUNT
    gc = gspread.service_account(filename=str(creds_path))
    sh = gc.open_by_key(config.GOOGLE_SHEET_ID)

    try:
        ws = sh.worksheet(config.COA_TAB_NAME)
        print(f'Successfully connected to sheet: {sh.title} | Tab Name: {config.COA_TAB_NAME}')
        return ws
    except gspread.exceptions.WorksheetNotFound:
        available = [ws.title for ws in sh.worksheets()]
        raise ValueError(f'Tab Name: {config.COA_TAB_NAME} not found, available: {available}')

# runs the row level validation through the pydantic model
def validate_coa_data(raw_records: list[dict]):
    validated_data = []
    error_logs = [] # ineligible rows go here for review
    
    # add / delete columns for .log file entries here
    log_columns = [
        'account_code', 'account_name', 'account_description', 
        'account_parent', 'account_main_category', 'account_sub_category', 
        'account_coa_category', 'account_in_expense_dashboard', 'account_dup_code'
    ]
    
    # pull and validate data
    for i, record in enumerate(raw_records):
        row_number = i + 2 # sheets row number
        try:
            # row level validation is applied here via ChartOfAccountsRow()
            obj = ChartOfAccountsRow(**record)
            validated_data.append(obj.model_dump(mode='json'))
        except ValidationError as e:
            row_values = " | ".join([f'{col}: {record.get(col, "MISSING")}' for col in log_columns])
            clean_errors = "; ".join([f"{err['loc'][0]}: {err['msg']}" for err in e.errors()])

            log_entry = (
                f'(ROW {row_number}) '
                f'DATA: {row_values}\n'
                f'ERROR: {clean_errors}\n'
                f"{'-'*100}\n"
            )
            error_logs.append(log_entry) # appends the actual logs that you see 
            
    return validated_data, error_logs

# writes validation errors onto the logfile for review
def write_ingestion_logs(error_logs: list[str]):
    if not error_logs:
        return
        
    # setting up the location of the log file 
    config.log_path_dir.mkdir(exist_ok=True)
    log_file = config.log_path_dir / 'coa_ingestion.logs' 

    with open(log_file, 'w', encoding='utf-8') as f:
        f.writelines(error_logs) # writes the log entries hehe
        
    print(f'Errors have been encountered, please check {Path(*log_file.parts[-2:])} for more details.')

# main function for ingesting data, where ingestion "happens" hehe 
def get_validated_data(): 
    # 0. visual divider
    print_divider("chart of accounts")

    # 1. connect to google
    worksheet = get_worksheet()

    # 2. pull raw records from sheet
    raw_records = worksheet.get_all_records()
    if not raw_records:
        print("No records found in sheet.")
        return pl.DataFrame()

    # 3. run validation loop
    validated_data, error_logs = validate_coa_data(raw_records)

    # 4. handle log entries
    write_ingestion_logs(error_logs)

    # 5. convert raw data into Polars df
    if not validated_data:
        print('No valid data found.')
        return pl.DataFrame()

    print(f"Validation complete. {len(validated_data)} rows cleared.")
    return pl.DataFrame(validated_data)