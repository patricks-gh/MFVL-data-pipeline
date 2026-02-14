import gspread
import polars as pl 
from pathlib import Path
from pydantic import ValidationError
import config
from models.expenses_01 import Expenses01Row
from datetime import datetime

# creates a visual break in the terminal logs
def print_divider(title: str):
    print(f'\n{'-'*90}')
    print(f'PROCESS: {title.upper()}')
    print(f'{'-'*90}')

# handles the google sheets connection
def get_worksheet() -> gspread.Worksheet:

    creds_path = config.shared_root / config.GOOGLE_SERVICE_ACCOUNT
    gc = gspread.service_account(filename=str(creds_path))
    sh = gc.open_by_key(config.GOOGLE_SHEET_ID)

    try:
        ws = sh.worksheet(config.EXPENSES01_TAB_NAME)
        print(f'Successfully connected to sheet: {sh.title} | Tab Name: {config.EXPENSES01_TAB_NAME}')
        return ws
    except gspread.exceptions.WorksheetNotFound:
        available = [w.title for w in sh.worksheets()]
        raise ValueError(f'Tab not found. Available: {available}')

# clean specific sheet strings that pydantic hates ($, commas)
def sanitize_record(record: dict) -> dict:
    amt = str(record.get('expense_amount', '0')).replace('$', '').replace(',', '').strip()
    record['expense_amount'] = amt if amt else '0'
    record['account_code'] = str(record.get('account_code', '0')).strip()
    
    for date_field in ['expense_record_date', 'expense_date']:
        val = str(record.get(date_field, '')).strip()
        if not val:
            record[date_field] = None
        else:
            try:
                # If it's in M/D/YYYY format, convert it to YYYY-MM-DD
                # This handles '1/20/2026' or '01/20/2026'
                if '/' in val:
                    # Check if it has time or just date
                    fmt = "%m/%d/%Y %H:%M:%S" if ' ' in val else "%m/%d/%Y"
                    dt_obj = datetime.strptime(val, fmt)
                    record[date_field] = dt_obj.isoformat()
                else:
                    # It's already in a good format, just pass it through
                    record[date_field] = val
            except Exception:
                record[date_field] = None
                
    return record

# runs the pydantic validation loop and collects errors
def validate_sheet_data(raw_records: list[dict]):
    validated_data = []
    error_logs = []
    log_cols = ['expense_record_date','expense_date','account_code','expense_description','expense_amount','expense_sender']

    for i, raw_record in enumerate(raw_records):
        row_number = i + 2 
        record = sanitize_record(raw_record)

        try:
            obj = Expenses01Row(**record)
            validated_data.append(obj.model_dump(mode='json'))
        except ValidationError as e:
            row_values = ' | '.join([f'{col}: {record.get(col,'MISSING')}' for col in log_cols])
            clean_errors = '; '.join([f'{err['loc'][0]}: {err['msg']}' for err in e.errors()])
            error_logs.append(f'(ROW {row_number}) DATA: {row_values}\nERROR: {clean_errors}\n{'-'*100}\n')
            
    return validated_data, error_logs

# writes validation errors to the logfile
def write_ingestion_logs(error_logs: list[str]):
    if not error_logs:
        return
    config.log_path_dir.mkdir(exist_ok=True)
    log_file = config.log_path_dir / 'expenses_ingestion.logs'
    with open(log_file, 'w', encoding='utf-8') as f:
        f.writelines(error_logs)
    print(f'NOTE: {len(error_logs)} validation errors found. Check logs/expenses_ingestion.logs')

# generates the custom transaction ids
def add_transaction_ids(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.int_range(2, df.height + 2)
        .map_elements(lambda x: f'EXP-LN-{x:06d}', return_dtype=pl.String)
        .alias('expense_transaction_id')
    )

# main function for ingesting data
def get_validated_data():
    # 0. visual divider
    print_divider('expenses ingestion')

    # 1. extract
    worksheet = get_worksheet()
    raw_records = worksheet.get_all_records()
    if not raw_records:
        print('Sheet is empty.')
        return pl.DataFrame()

    # 2. transform & validate
    validated_data, error_logs = validate_sheet_data(raw_records)
    write_ingestion_logs(error_logs)

    # 3. load into polars
    if not validated_data:
        print('No valid data found to process.')
        return pl.DataFrame()

    print(f'Validation complete. {len(validated_data)} rows cleared.')
    
    df = pl.DataFrame(validated_data)
    return add_transaction_ids(df)