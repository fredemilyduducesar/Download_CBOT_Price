import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

def add_columns(df, name, ticker_symbol):
    """
    To add below columns in dataframe provided:
        1. Download time
        2. RunId (Eg: 2024-01-07 is 240107)
        3. ticker
        4. Name
    """
    # Add columns
    df['Download_time'] = datetime.now()
    df['RunId'] = datetime.today().strftime("%y%m%d")
    df['Ticker'] = ticker_symbol
    df['Name'] = name
    

def download_price_data(name, ticker_symbol, start_date, end_date, interval):
    """ 
    Download historical prices for a certain ticker symbol in time of interest, with designated interval and add download time and ID column.
    """
    # 1. Decode interval
    interval = '1wk' if interval == 'w' else '1d' if interval == 'd' else interval
    # 2. Decode date (make end_date 1 day older)
    end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days = 1)
    # 3. Download
    df = yf.download(ticker_symbol, start=start_date,
                       end=end_date, interval=interval)
    df = df.reset_index()
    # 4. Add columns
    add_columns(df, name, ticker_symbol)
    return df

def download_data(names, start_date, end_date, interval):
    """
    To download price data for a dictionary of product names, append the result into a dataframe
    """
    df_result=pd.DataFrame()
    for name in names:
        df = download_price_data(name, names[name], start_date, end_date, interval)
        df_result = df_result._append(df, ignore_index = True)
    df_result['Intraday_Id'] = range(1, df_result.shape[0] + 1)
    return df_result