# DownloadCBOTPrices
Download CBOT product prices, only for main contract prices.
Note: All products and corresponding ticker are configured in begining of main.py

# Parameters:
1. Start date: for example, 2024-01-02
2. End date: for example, 2024-01-10
3. Interval: 'd' means daily data; 'w' means weekly data. Only accept these two 

# Example run:

1. python main.py d:
    Download daily price data from 2000-01-01 to today+1, and write into DB (will replace the table if it already exists)

2. python main.py w:
    Download weekly price data from 2000-01-01 to today+1, and write into DB (will replace the table if it already exists)

3. python main.py 2024-01-10 d:
    Download daily price data for date 2024-01-10. 
    Duplication check enabled here, which means if the date is already exist in DB table, then it would not insert again

4. python main.py 2024-01-10 w:
    Same to above, except this is for weekly data

5. python main.py 2024-01-01 2024-01-10 d:
    Download daily price data from date 2024-01-01 to 2024-01-10 (but not include).
    Duplication check enabled here, which means for each date between 2024-01-01 and 2024-01-10 here, if it is already exist in DB table, then it would not insert again.

6. python main.py 2024-01-01 2024-01-10 w:
    Same as above. Except for weekly data here.
