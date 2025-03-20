import pandas as pd
import requests
import json
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2 
import io
import yaml

# Import the yfinance. If you get module not found error the run !pip install yfinance from your Jupyter notebook
import yahoo_fin as yf
import yahoo_fin.stock_info as si

sp500_list = si.tickers_sp500()

config = yaml.safe_load(open("localpgcreds.yml"))

def cleanStockData(dict_f, tickers):
    for ticker in tickers:
        if ticker in dict_f.keys():
            df1 = dict_f[ticker]
            df2 = df1.drop(columns=['open', 'adjclose'], axis=1)
            df2 = df2.rename_axis('tradedate')
            df2 = df2.rename(columns={'close': 'closeprice'})
            dict_f[ticker] = df2

engine = create_engine(f"postgresql+psycopg2://{config['username']}:{config['dbpass']}@{config['server']}/{config['dbname']}")

def insert_df_pgsql(df, table_name):
    # Drop old table and create new empty table
    #df.head(0).to_sql(table_name, engine, if_exists='replace',index=True)
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=True)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table_name, null="") # null values become ''
    conn.commit()
    cur.close()
    conn.close()
	
def fillHistoricalData(date1, date2, tickers):
    df_list = {}
    headers = {'User-agent': 'Mozilla/5.0'}
    for ticker in tickers:
        df_list[ticker] = si.get_data(ticker, start_date=date1, end_date=date2, index_as_date = True)
    return df_list

#get date last ran
connection = engine.raw_connection()
cursor = connection.cursor()
postgreSQL_select_Query = "select MAX(tradedate) from stocks GROUP BY ticker LIMIT 1"
cursor.execute(postgreSQL_select_Query)
date_db = cursor.fetchone()
date_last = date_db[0].strftime('%m/%d/%y')
cursor.close()
connection.close()
#if we haven't ran it before, use the first of the current month
if date_last == "":
	date_last = datetime.today().replace(day=1).strftime("%m/%d/%Y")

date_today = datetime.today().strftime("%m/%d/%Y")
sp500_data = fillHistoricalData(date_last, date_today, sp500_list)
cleanStockData(sp500_data, sp500_list)
#each stock comes as a separate data frame
for ticker in sp500_list:
    insert_df_pgsql(sp500_data[ticker],'stocks')