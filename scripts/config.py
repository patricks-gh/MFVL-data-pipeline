import os
from pathlib import Path
from dotenv import load_dotenv

# environment variables and directory setup 
this_script = Path(__file__).resolve() #absolute path of this script 
shared_root = this_script.parents[3] 
project_root = this_script.parents[1] # rawdata_ingestion folder
log_path_dir = project_root / 'logs' #logfile parent directory
dotenv_path = shared_root / 'keys' / '.env'

if not dotenv_path.exists():
    raise FileNotFoundError(f'Unable to find .env at: {dotenv_path}')

load_dotenv(dotenv_path)

# load settings from .env file 
GOOGLE_SERVICE_ACCOUNT = os.getenv('GOOGLE_API_SERVICE_ACCOUNT') #variable name used in .env
GOOGLE_SHEET_ID = os.getenv('COA_GOOGLE_SHEET_ID') #variable name used in .env
COA_TAB_NAME = os.getenv('COA_TAB_NAME')
EXPENSES02_TAB_NAME = os.getenv('EXPENSES02_TAB_NAME')
EXPENSES01_TAB_NAME = os.getenv('EXPENSES01_TAB_NAME')
INVOICES01_TAB_NAME = os.getenv('INVOICES01_TAB_NAME')
RECURRING01_TAB_NAME = os.getenv('RECURRING01_TAB_NAME')

# Supabase (for the next step)
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')