# ===============================================Call Packages======================================================
import os
import time
import pandas as pd
import datetime as dt
from datetime import datetime
from datetime import timedelta

# Credential Parameters
from etl_init import DB_CONFIG

# Utility functions
from utils.database_util import connect
from utils.database_util import copy_from_dataFile_1m
from utils.finnhub_util import get_forex_candles
from utils.notification_util import email_alert

# ===============================================Load Database Parameters============================================
conn_params_dic = {
                    "host": DB_CONFIG['db_host'],
                    "port": 5555,
                    "database": DB_CONFIG['db_name'],
                    "user": DB_CONFIG['db_user'],
                    "password": DB_CONFIG['db_pass']
                  }



# ==============================================Get the list of symbols==============================================
# declare a cursor object from the connection
conn = connect(conn_params_dic)
cursor = conn.cursor()
# Execute query - getting the list of all forex symbols:
sql = "SELECT DISTINCT symbol FROM forex_precious_metal_symbol_jason_stage"
cursor.execute(sql)

# Fetch all the records
tuples = cursor.fetchall()
existing_table = pd.DataFrame(tuples,columns=['Symbol'])

# Convert the tuples to array, this is used in the FOR loop in next section
array = existing_table[['Symbol']].to_numpy()

# ==============================================Determine the loading condition=======================================
# Get the latest trading date from the table forex_precious_metal_1m_jason_stage
# declare a cursor object from the connection
conn = connect(conn_params_dic)
cursor = conn.cursor()
# Execute query 
sql = "SELECT MAX(time_stamp_nyc) FROM forex_precious_metal_1m_jason_stage"
cursor.execute(sql)

# Fetch the latest trading date
latest_daily_candle = cursor.fetchall()[0][0]  

# Get the latest trading time from EUR_USD
now = datetime.now()
now_240_hrs = now - timedelta(hours=240)
# Convert to unix timestamp
dt0_now_240_hrs = int(now_240_hrs.timestamp())
dt1_now = int(now.timestamp())
interval = '1'
df_r = get_forex_candles('OANDA:EUR_USD', interval, dt0_now_240_hrs, dt1_now)
latest_trading_date = df_r['ts'].max()  
latest_trading_date = datetime.strptime(str(latest_trading_date), '%Y-%m-%d %H:%M:%S') 


# If-else condition - If the latest trading datetime from Finnhub is later than the max datetime in database,
# then data is not up to date. We need to load the data.

# ==============================================Load daily candle data================================================
start = time.time()

# If lastest trading date from Finnhub is Friday, then use different logic
if latest_trading_date.weekday() == 4:  # if on Friday
    dt0 = int((latest_daily_candle + timedelta(minutes=1)).timestamp()) # Need to add 1 minute - otherwise creating duplicates
    dt1 = int((latest_trading_date + timedelta(minutes=60)).timestamp()) # Need to add 60 minutes - becasue Friday is closing market at 5PM

else:
    dt0 = int((latest_daily_candle + timedelta(minutes=1)).timestamp()) # Need to add 1 minute - otherwise creating duplicates
    dt1 = int((latest_trading_date - timedelta(minutes=5)).timestamp()) # Need to minus 5 minutes - avoid insufficient trading volume

interval = '1'

# Define FOR Loop
if latest_trading_date > latest_daily_candle:
    body = 'Not up to date, we need to do incremental load.'
    print(body)
        
    # Loading loop starts
    for s in array:
        print ('Updating: ',s , '...........')
            
        try:
            df_r = get_forex_candles(s, interval, dt0, dt1)
            df_r = df_r.rename(columns={"c": "close_price", 
                                        "h": "high_price",
                                        "l": "low_price", 
                                        "o": "open_price",
                                        "s": "status", 
                                        "t": "time_stamp_unix",
                                        "v": "volume", 
                                        "ts": "time_stamp_nyc",
                                        "symbol": "forex_symbol",
                                        } 
                              )          

            # Load the temp df into Database table
            # Connect to the database
            conn = connect(conn_params_dic)
            conn.autocommit = True

            # Run the copy_from_dataFile method, here saving data into forex_precious_metal_1m_jason_stage
            copy_from_dataFile_1m(conn, df_r, 'forex_precious_metal_1m_jason_stage')

            # My primium acount is 150 API calls/minute
            # Pause for 0.5 second
            print('Pause for 0.5 seconds............')
            time.sleep(0.5)

            # Close the connection
            conn.close()

            # Remove the temp file
            tmp_df = "df_temp_daily.csv"
            os.remove(tmp_df)

        except Exception:
            continue
else:
    body = 'Data is up to date, no need to do incremental load.'
    print(body)

    # Close the connection
    conn.close()

# Loading loop ends

end = time.time()

Elapsed_Time = (end - start)
print('Time Elapsed: ', end - start)



# ==========================================Send Email Alert Notification===================================================
msg = f"The ETL job for refreshing forex_precious_metal_1m_jason_stage table is completed on {dt.date.today().strftime('%Y/%m/%d')}. \n" \
      f"Sending at {dt.datetime.now().strftime('%H:%M:%S.%f')}," \
      f" and elapsed time is {Elapsed_Time}. \n" \
      f"Update Message: {body} "

if __name__ == '__main__':
    email_alert("Algo Trading ETL process update", msg, "emailalertjasonlu900625@gmail.com;algotraders.investors@gmail.com")

