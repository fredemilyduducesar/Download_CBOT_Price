# DownloadCBOTPrices

Download CBOT product prices, only for main contract prices.
Note: All products and corresponding ticker are configured at the begining of main.py

## Parameters:
1. Start date: for example, 2024-01-02
2. End date: for example, 2024-01-10
3. Interval: 'd' means daily data; 'w' means weekly data. Only accept these two 

## Setup
bash
pip install -r requirements.txt
cp .env.example .env
### edit .env to set DB_SERVER/DB_DATABASE/DB_SCHEMA

## Run:

python main.py d

python main.py w

python main.py 2024-01-10 d

python main.py 2024-01-01 2024-01-10 d

