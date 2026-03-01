import gspread
import polars as pl
from pathlib import Path
from pydantic import ValidationError
import config
from models.recurring_01 import Recurring01Row
from datetime import datetime, date

# creates a visual break in the terminal logs
def print_divider(title: str):
    print(f'\n{'-'*90}')
    print(f' PROCESS: {title.upper()} ')
    print(f'{'-'*90}')

# handles the google sheets connection
def get_worksheet() -> gspread.Worksheet:
    creds_path = config.shared_root / config.GOOGLE_SERVICE_ACCOUNT
    gc = gspread.service_account(filename=str(creds_path))
    sh = gc.open_by_key(config.GOOGLE_SHEET_ID)

    try:
        ws = sh.worksheet(config.RECURRING01_TAB_NAME)
        print(f'Succesfully connected to sheet: {sh.title} | Tab Name: {config.RECURRING01_TAB_NAME}')
        return ws
    except gspread.exceptions.WorksheetNotFound:
        available = [w.title for w in sh.worksheets()]
        raise ValueError(f'Tab not found. Available: {available}')

def sanitize_record(record: dict) -> dict:
    # 1. Clean Numbers (Required)
    amt = str(record.get('recurring_fee_amount', '0')).replace('$', '').replace(',', '').strip()
    record['recurring_fee_amount'] = amt if amt else '0'
    record['recurring_fee_account_code'] = str(record.get('recurring_fee_account_code', '0')).strip()

    # 2. Clean Required Strings
    for req_str in ['recurring_fee_name', 'recurring_fee_status', 'recurring_fee_payment_status', 'recurring_fee_payment_terms']:
        record[req_str] = str(record.get(req_str, '')).strip()

    # 3. Clean Optional Strings (Strict NULL logic)
    for opt_str in ['recurring_fee_type', 'recurring_fee_contract_duration', 'recurring_fee_comment']:
        val = str(record.get(opt_str, '')).strip()
        record[opt_str] = val if val else None

    # --- DATE FIELDS ---
    for date_field in ['recurring_fee_record_date', 'recurring_fee_date']:
        val = str(record.get(date_field, '')).strip()
        if not val:
            record[date_field] = None
        else:
            try:
                # 1. Handle AM/PM logic (Common in Google Sheets)
                if 'AM' in val.upper() or 'PM' in val.upper():
                    # Check for dash (2026-04-02) or slash (04/02/2026)
                    date_part = "%Y-%m-%d" if '-' in val else "%m/%d/%Y"
                    # We use %I for 12-hour clock
                    fmt = f"{date_part} %I:%M:%S %p"
                    record[date_field] = datetime.strptime(val, fmt).isoformat()
                
                # 2. Handle standard Slotted dates (01/01/2026)
                elif '/' in val:
                    fmt = "%m/%d/%Y %H:%M:%S" if ' ' in val else "%m/%d/%Y"
                    record[date_field] = datetime.strptime(val, fmt).isoformat()
                
                # 3. Handle ISO dates already in string format
                else:
                    # This handles '2026-01-01'
                    record[date_field] = val 
            except Exception as e:
                # Log the error to terminal so you can see why it failed
                print(f"DEBUG: Date conversion failed for '{val}' in {date_field}. Error: {e}")
                record[date_field] = None
                
    return record

def validate_sheet_data(raw_records: list[dict]):
    validated_data = []
    error_logs = []
    log_cols = ['recurring_fee_record_date','recurring_fee_date','recurring_fee_name','recurring_fee_amount','recurring_fee_status','recurring_fee_payment_status', 'recurring_fee_account_code', 'recurring_fee_payment_terms']

    for i, raw_record in enumerate(raw_records):
        row_number = i + 2 
        record = sanitize_record(raw_record)

        try:
            obj = Recurring01Row(**record)
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
    log_file = config.log_path_dir / 'recurring_fee_ingestion.logs'
    with open(log_file, 'w', encoding='utf-8') as f:
        f.writelines(error_logs)
    print(f'NOTE: {len(error_logs)} validation errors found. Check logs/recurring_fee_ingestion.logs')

# generates the custom transaction ids
def add_transaction_ids(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.int_range(2, df.height + 2)
        .map_elements(lambda x: f'RCR-LN-{x:06d}', return_dtype=pl.String)
        .alias('recurring_fee_transaction_id')
    )

# main function for ingesting data
def get_validated_data():
    # 0. visual divider
    print_divider('recurring fee ingestion')

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