import gspread
import polars as pl
from pathlib import Path
from pydantic import ValidationError 
import config
from models.invoices_01 import Invoices01Row

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
        ws = sh.worksheet(config.INVOICES01_TAB_NAME)
        print(f'Succesfully connected to sheet: {sh.title} | Tab Name: {config.INVOICES01_TAB_NAME}')
        return ws
    except gspread.exceptions.WorksheetNotFound:
        available = [w.title for w in sh.worksheets()]
        raise ValueError(f'Tab not found. Available: {available}')


def sanitize_record(record: dict) -> dict:
    # --- REQUIRED FIELDS ---
    total_cost = str(record.get('invoice_total_cost', '0')).replace('$', '').replace(',', '').strip()
    record['invoice_total_cost'] = total_cost if total_cost else '0'
    record['account_code'] = str(record.get('account_code', '0')).strip()

    # --- NUMERIC FIELDS (Optional) ---
    for num_field in ['invoice_unit_price', 'invoice_qty']:
        val = str(record.get(num_field, '')).replace('$', '').replace(',', '').strip()
        record[num_field] = float(val) if val else None

    # --- STRING FIELDS (Optional) ---
    optional_strings = [
        'invoice_item', 'invoice_description', 'invoice_name', 
        'invoice_comments', 'invoice_supplier_name', 'invoice_unit_type'
    ]
    for s_field in optional_strings:
        val = str(record.get(s_field, '')).strip()
        record[s_field] = val if val else None

    # --- DATE FIELDS (With Formatter) ---
    for date_field in ['invoice_record_date', 'invoice_date']:
        val = str(record.get(date_field, '')).strip()
        if not val:
            record[date_field] = None
        else:
            try:
                # If Google Sheets sends M/D/YYYY, convert to ISO
                if '/' in val:
                    fmt = "%m/%d/%Y %H:%M:%S" if ' ' in val else "%m/%d/%Y"
                    record[date_field] = datetime.strptime(val, fmt).isoformat()
                else:
                    record[date_field] = val # Already ISO format
            except Exception:
                record[date_field] = None # Fallback to NULL if date is gibberish
                
    return record


def validate_sheet_data(raw_records: list[dict]):
    validated_data = []
    error_logs = []
    log_cols = ['invoice_record_date','invoice_date','invoice_item','invoice_total_cost','invoice_description','invoice_name', 'account_code']

    for i, raw_record in enumerate(raw_records):
        row_number = i + 2 
        record = sanitize_record(raw_record)

        try:
            obj = Invoices01Row(**record)
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
    log_file = config.log_path_dir / 'invoices_ingestion.logs'
    with open(log_file, 'w', encoding='utf-8') as f:
        f.writelines(error_logs)
    print(f'NOTE: {len(error_logs)} validation errors found. Check logs/invoices_ingestion.logs')


# generates the custom transaction ids
def add_transaction_ids(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.int_range(2, df.height + 2)
        .map_elements(lambda x: f'INV-LN-{x:06d}', return_dtype=pl.String)
        .alias('invoice_transaction_id')
    )


# main function for ingesting data
def get_validated_data():
    # 0. visual divider
    print_divider('invoices ingestion')

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