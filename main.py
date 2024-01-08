"""
Main program
"""
import sys
from get_dates_freq_and_check import validate_parameters
from download_data import download_data
from write_into_DB import write_data_into_db, get_existing_dates, data_duplication_removal
import time
import logging

logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')

# 1. Configurations
server = 'FRED' 
database = 'Raw'
schema = 'CBOT'
name_mapping = { # List of products for download with corresponding ticker 
    "Soybean Meal": "ZS=F",
    "Corn": "ZC=F",
    "Soybean Oil": "ZL=F",
    "Lean Hogs": "HE=F",
    "Live Cattle": "LE=F",
    "Cocoa": "CC=F",
    "Coffee": "KC=F",
    "Cotton": "CT=F",
    "Orange Juice": "OJ=F",
    "Sugar": "SB=F"
}

# 2. Functions
def display_runtime(total_time_seconds):
    """
    Display time of execurtion
    """
    minutes = int(total_time_seconds // 60)
    seconds = int(total_time_seconds % 60)
    milliseconds = int((total_time_seconds - minutes * 60 - seconds) * 1000)

    print(f"The time of execution is: {minutes} mins {seconds} s {milliseconds} ms")
    
def main():
    """
    Main function to start.
    """
    # 1. Check parameters
    logging.info("****** 1. Process Starts - get paras & check ******")

    start_date, end_date, freq = validate_parameters(sys.argv)
    database_tablename = 'Daily_Price_Data' if freq.lower() == 'd' else 'Weekly_Price_Data'

    # 2. Download data
    logging.info("****** 2. Download Starts ******")
    
    data = download_data(name_mapping, start_date, end_date, freq)
    
    if data.empty:
        logging.info("****** 3. No data downloaded. Process finished. ******")
        return

    # 3. Duplication dates removal
    existing_dates = get_existing_dates(server, database, schema, database_tablename)
    data = data_duplication_removal(existing_dates, data)
    
    if data.empty:
        logging.info("****** 3. No data left after date duplication removal. No data need to write in DB. Process Finish. ******")
        return
    
    # 4. Wrie into DB
    logging.info("****** 3. Starts to write data into DB ******")
    
    write_data_into_db(data, server, schema, database, database_tablename)

    # 4. Complete
    logging.info(f"****** 4. Process completes ******")
    logging.info(f"Total {data.shape[0]} records inserted into: {server}.{database}.{schema}.{database_tablename}")

# 3. Start
if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    display_runtime(end_time - start_time)
