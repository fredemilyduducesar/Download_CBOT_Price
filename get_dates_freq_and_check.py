"""
This package contains functions that used to validate parameters 
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