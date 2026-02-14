'''
DUAL LOGGING
------------
this scripts redirects all standard output and errors to both the terminal and a local log file.

KEY NOTES:
- overwrites 'main_ingestions.log' on every run.
- also captures system crashes from stderr that would otherwise be lost. 
- uses 'Tee' class just to somewhat mimic the Unix tee command hehe.

'''

import sys
import config

class Tee(object):
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, 'w', encoding='utf-8')
    
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    
    def flush(self):
        self.terminal.flush()
        self.log.flush()

def setup_logging():
    config.log_path_dir.mkdir(exist_ok=True)
    log_file = config.log_path_dir / 'main_ingestion.log'
    sys.stdout = Tee(log_file)
    sys.stderr = sys.stdout
    return log_file