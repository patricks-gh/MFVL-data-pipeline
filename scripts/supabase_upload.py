'''
SUPABASE LOADER
---------------
Universal uploader script for the entire ingestion pipeline. 
No matter which sheet the data comes from, it passes through here to enter the cloud.

KEY NOTES:
- Dynamic Routing: Automatically assigns the correct database schema (accounting vs. expenses) based on the table name.
- Upsert Logic: Uses 'on_conflict' IDs to prevent duplicate rows, it updates existing records and inserts new ones.
- Timestamping: Injects a 'record_updated_at' column in PST so we can track exactly when the data was synced, regardless of when it was created.

'''

from supabase import create_client, Client
import config
import polars as pl
import datetime
import pytz

# Table Name: (Schema, Primary Key, Timestamp Column)
TABLE_CONFIGS = {
    'chart_of_accounts': ('accounting', 'account_code', 'account_updated_at'),
    'latest_expenses_01': ('expenses', 'expense_transaction_id', 'expense_record_updated_at'),
    'latest_expenses_02': ('expenses', 'expense_transaction_id', 'expense_record_updated_at'),
    'latest_invoices_01': ('expenses', 'invoice_transaction_id', 'invoice_record_updated_at'),
    'latest_recurring_fees_01': ('expenses', 'recurring_fee_transaction_id', 'recurring_fee_record_updated_at'),
}

PST = pytz.timezone('America/Los_Angeles')

def upload_to_supabase(df: pl.DataFrame, table_name: str):

    # checking if df is empty before doing anything 
    if df.is_empty():
        print(f'No data available for {table_name}, please verify and try again...')
        return

    # resolve config 
    schema, on_conflict_id, timestamp_col = TABLE_CONFIGS.get(
            table_name, ('public', 'id', 'updated_at')
        )
    
    # capture current execution time for timestamping 
    now = datetime.datetime.now(PST)
    pst_now_str = now.strftime('%b %d, %Y at %I:%M %p')

    df = df.with_columns(pl.lit(now.isoformat()).alias(timestamp_col)) # adds the timestamp column to the dataframe

    # initializing the client
    url, key = config.SUPABASE_URL, config.SUPABASE_KEY
    if not url or not key:
        raise EnvironmentError("Supabase credentials missing in .env config.")
    
    supabase: Client = create_client(url, key)
    
    # execute upsert to supabase
    records = df.fill_null(pl.lit(None)).to_dicts() #ensures database compatibility for empty cells
    print(f'{len(records)} rows available. Uploading to {schema}.{table_name}')

    try: 
        response = (
            supabase.schema(schema)
            .table(table_name)
            .upsert(records, on_conflict=on_conflict_id)
            .execute()
        )
        print(f"SUCCESS: Uploaded to Supabase at {pst_now_str} PST.")
        return response

    except Exception as e:
        print(f"FAILURE: Supabase upload failed for {table_name}.")
        print(f"DEBUG: {str(e)}")
        raise

