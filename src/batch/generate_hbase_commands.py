import sys
import os

try:
    from src.config_tickers import NIFTY_500_SECTORS, NIFTY_500_NAMES
except ModuleNotFoundError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from config_tickers import NIFTY_500_SECTORS, NIFTY_500_NAMES

def main():
    print("echo 'Starting bulk insert to HBase...'")
    for sym, name in NIFTY_500_NAMES.items():
        # Escape single quotes in name for hbase shell
        safe_name = name.replace("'", "\\'")
        print(f"put 'ticker_metadata', '{sym}', 'info:name', '{safe_name}'")
        
    for sym, sec in NIFTY_500_SECTORS.items():
        safe_sec = sec.replace("'", "\\'")
        print(f"put 'ticker_metadata', '{sym}', 'sector:name', '{safe_sec}'")
        
    print("scan 'ticker_metadata', {LIMIT => 5}")
    print("exit")

if __name__ == "__main__":
    main()
