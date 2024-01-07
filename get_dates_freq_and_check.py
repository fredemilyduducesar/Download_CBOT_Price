"""
This package contains functions that used to:
    1. Validate parameters
    2. Get parameters (dates, freq)
    3. Check parameters to exclude dates that have been written into DB 
"""
import datetime
import urllib
from sqlalchemy import create_engine, text
import pandas as pd
import logging

logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')

# 1. Functions for validating parameters
def validate_freq(freq):
    """
    Validate frequency is either 'd' or 'w'.
    """
    if not isinstance(freq, str) or freq.lower() not in ['d', 'w']:
        raise ValueError("Frequency must be 'd' or 'w', case insensitive.")
    logging.info("Frequency validated.")

def validate_date_format(date_str):
    """
    Validate date is in YYYY-MM-DD format.
    """
    try:
        datetime.datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Date must be in YYYY-MM-DD format.")
    logging.info("Date format validated.")

def validate_date_order(start_date, end_date):
    """
    Validate start_date is not after end_date.
    """
    if start_date > end_date:
        raise ValueError("Start date must not be after end date.")
    logging.info("Date order validated.")

def validate_parameters(args):
    """
    Validate parameters based on the number and type.
    """
    num_args = len(args)
    if num_args not in range(2, 5):
        raise ValueError("Invalid number of arguments.")

    freq = args[-1]
    validate_freq(freq)

    dates = args[1:-1]
    for date in dates:
        validate_date_format(date)

    if len(dates) == 2:
        start_date, end_date = map(lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'), dates)
        validate_date_order(start_date, end_date)

    elif len(dates) == 1:
        start_date = end_date = datetime.datetime.strptime(dates[0], '%Y-%m-%d')
    else:
        start_date = end_date = datetime.datetime.today()

    logging.info(f"Parameters validated: ");
    logging.info(f"Start date: {start_date.strftime('%Y-%m-%d')}, End date: {end_date.strftime('%Y-%m-%d')}, Frequency: {freq}")
    return start_date, end_date, freq

# 2. Functions for checking parameters
def get_existing_dates(server, database, schema, database_tablename):
    """
    To get existing dates from table in sql server database
    """
    # Concat string
    params = urllib.parse.quote_plus(
        "Driver={ODBC Driver 17 for SQL Server};"
        f"Server={server};"
        f"Database={database};"
        "Trusted_Connection=yes;"
    )
    connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
    # Connect to database
    engine = create_engine(connection_string)
    # Get distinct dates
    query = f"""
        IF EXISTS (
            SELECT * 
            FROM {database}.information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_name = '{database_tablename}' 
            AND table_catalog = '{database}'
        )
        BEGIN
            SELECT DISTINCT Date 
            FROM {database}.{schema}.{database_tablename} 
            ORDER BY Date
        END
    """
    try:
        existing_dates = pd.read_sql(query, engine)
        return existing_dates['Date'].tolist()
    except:
        # If an error occurs (e.g., empty result set), return an empty list
        return []

def generate_date_range(start_date, end_date, freq):
    """
    Generate a date range based on the given frequency.

    :param start_date: Start date in 'YYYY-MM-DD' format.
    :param end_date: End date in 'YYYY-MM-DD' format.
    :param freq: Frequency - 'd' or 'D' for daily, 'w' or 'W' for weekly (Fridays).
    :return: Pandas DatetimeIndex of dates.
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    if freq.lower() == 'd':
        return pd.date_range(start, end, freq='B')  # Business days
    elif freq.lower() == 'w':
        return pd.date_range(start, end, freq='W-FRI')  # Only Fridays
    else:
        raise ValueError("Invalid frequency. Use 'd' or 'D' for daily, 'w' or 'W' for weekly.")

def check_existing_dates(dates, existing_dates, freq):
    """
    Check and print if dates exist in existing_dates and return dates not in existing_dates.

    :param dates: Pandas DatetimeIndex of dates.
    :param existing_dates: List of dates that should be excluded.
    :param freq: Frequency - 'd' for daily, 'w' for weekly (Fridays).
    :return: List of dates not in existing_dates.
    """
    final_dates = []
    for date in dates:
        date_str = date.strftime('%Y-%m-%d')
        if date_str in existing_dates:
            if freq.lower() == 'd':
                logging.info(f"Date {date_str} is existed already, ignore.")
            else:
                week_num = date.isocalendar()[1]
                year = date.isocalendar()[0]
                logging.info(f"Date {date_str} ({year}, {week_num} week) is existed already, ignore.")
        else:
            final_dates.append(datetime.datetime.strptime(date_str, '%Y-%m-%d'))
    return final_dates

def construct_dates(start_date, end_date, freq, existing_dates):
    """
    Construct a list of dates from start_date to end_date based on the given frequency.
    Exclude dates that are in the existing_dates list.

    :param start_date: Start date in 'YYYY-MM-DD' format.
    :param end_date: End date in 'YYYY-MM-DD' format.
    :param freq: Frequency - 'd' or 'D' for daily, 'w' or 'W' for weekly (Fridays).
    :param existing_dates: List of dates that should be excluded.
    :return: List of dates not in existing_dates.
    """
    dates = generate_date_range(start_date, end_date, freq)
    return check_existing_dates(dates, existing_dates, freq)

# 3. Put together
def get_dates_freq_and_check(start_date, end_date, freq, server, database, schema, database_tablename):
    """
    This function is to return a list of dates that need to be downloaded
    """
    existing_dates = get_existing_dates(server, database, schema, database_tablename)
    return construct_dates(start_date, end_date, freq, existing_dates)