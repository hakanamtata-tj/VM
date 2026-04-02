import subprocess
import time
import datetime
import os
import sys
import logging
from config import LOG_FILE
from commonFunction import is_today_holiday
from telegrambot import send_telegram_message
# Base directory where all scripts are located
BASE_DIR = "/home/harshilkhatri2808/pmk_all_in_one"
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(
    filename=LOG_FILE,
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def run_script(script_name):
    """Run a script and return True if success, False if failed"""
    script_path = os.path.join(BASE_DIR, script_name)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] ?? Running {script_path}...")
    result = subprocess.run([sys.executable, script_path])
    if result.returncode != 0:
        print(f"[{timestamp}] ? {script_name} failed!")
    return result.returncode == 0

if __name__ == "__main__":
    today = datetime.date.today().isoformat()
    last_run_file = os.path.join(BASE_DIR, "last_run.txt")

    if is_today_holiday():
        print("Today is a holiday. Exiting the process.")
        logging.info("Today is a holiday. Exiting the process.")
        send_telegram_message("Today is a holiday. No scripts were run.")
        exit(0)

    # Check if already run today
    last_run_date = None
    if os.path.exists(last_run_file):
        with open(last_run_file, "r") as f:
            last_run_date = f.read().strip()

    if last_run_date != today:
        # First run of the day ? run kitelogin + updateinstrument + emalive
        if run_script('kitelogin.py'):
            time.sleep(1)
            if run_script('updateinstrument.py'):
                time.sleep(1)
                if run_script('emalive.py'):
                    # Only mark success if all scripts succeed
                    print(f"? All scripts ran successfully at {today}")
                    with open(last_run_file, "w") as f:
                        f.write(today)
                else:
                    print("? emalive.py failed")
            else:
                print("? updateinstrument.py failed")
        else:
            print("? kitelogin.py failed")
    else:
        # Already ran today ? only run emalive
        if run_script('emalive.py'):
            print("? emalive.py ran successfully")
        else:
            print("? emalive.py failed")
