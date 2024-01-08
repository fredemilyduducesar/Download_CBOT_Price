"""
This module contains functions to:
    1. Eliminate data of dates that already in DB
    2. Check schema existence
    3. Write into DB
"""
import urllib
from sqlalchemy import create_engine, text
import pandas as pd
import logging
import datetime

logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')

# 1. Date elimination
def get_existing_dates(server, database, schema, database_tablename):
    """
    To get a list of existing dates from table in sql server database
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
    
def data_duplication_removal(existing_dates, dataframe):
    """
    To remove dates that already exist in DB, create a new dataframe
    """
    # Convert the dates_to_remove to datetime objects for accurate comparison
    existing_dates = pd.to_datetime(existing_dates)
    
    # Convert the 'Date' column of the dataframe to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(dataframe['Date']):
        dataframe['Date'] = pd.to_datetime(dataframe['Date'])
    
    # Log and remove the dates
    for date in existing_dates:
        if date in dataframe['Date'].values:
            logging.info(f"Date {date.strftime('%Y-%m-%d')} is removed.")
            dataframe = dataframe[dataframe['Date'] != date]
            
    return dataframe

# 2. Check schema existence
def check_schema_existence(engine, database, schema):
    """
    Check schema exists or not, create one if not exist
    """
    with engine.connect() as conn:
        schema_creation_query = text(f"""
            USE {database}
            IF NOT EXISTS (
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = '{schema}'
            )
            BEGIN
                EXEC('CREATE SCHEMA {schema}')
            END
        """)
        conn.execute(schema_creation_query)
        
# 3. Write into DB
def write_data_into_db(dataframe, server, schema, database, database_tablename):
    """
    Write dataframe into database
    """
    # 1. Concat string
    params = urllib.parse.quote_plus(
        "Driver={ODBC Driver 17 for SQL Server};"
        f"Server={server};"
        f"Database={database};"
        "Trusted_Connection=yes;"
    )
    connection_string = f"mssql+pyodbc:///?odbc_connect={params}"

    # 2. Connect to database
    engine = create_engine(connection_string)

    # 3. Check schema existence
    check_schema_existence(engine, database, schema)
    
    # 4. Duplicate date removal
    existing_dates = get_existing_dates(server, database, schema, database_tablename)
    dataframe = data_duplication_removal(existing_dates, dataframe)

    # 4. Write into DB
    dataframe.to_sql(database_tablename, con=engine, schema=schema, index=False, if_exists='append')

    # 5. Close the connection
    engine.dispose()