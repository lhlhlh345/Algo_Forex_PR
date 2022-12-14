
### Run everyday anytime between 5:30PM - 11:59PM

# ===============================================Call Packages======================================================
import os
import time
import pandas as pd
import datetime as dt
from datetime import timedelta
from datetime import datetime

# Credential Parameters
from etl_init import DB_CONFIG

# Utility functions
from utils.database_util import connect
from utils.database_util import copy_from_dataFile_daily
from utils.finnhub_util import get_forex_candles
from utils.notification_util import email_alert

# ===============================================Load Database Parameters============================================
conn_params_dic = {
                    "host": DB_CONFIG['db_host'],
                    "port": 12345,
                    "database": DB_CONFIG['db_name'],
                    "user": DB_CONFIG['db_user'],
                    "password": DB_CONFIG['db_pass']
                  }


# ==============================================Get the list of symbols==============================================
# declare a cursor object from the connection
conn = connect(conn_params_dic)
cursor = conn.cursor()
# Execute query - getting the list of all forex symbols:
sql = "SELECT DISTINCT symbol FROM [YOUR SYMBOL TABLE]"
cursor.execute(sql)

# Fetch all the records
tuples = cursor.fetchall()
existing_table = pd.DataFrame(tuples,columns=['Symbol'])

# Convert the tuples to array, this is used in the FOR loop in next section
array = existing_table[['Symbol']].to_numpy()


# ==============================================Determine the loading condition=======================================
# Get the latest trading date from the table [YOUR DAILY TABLE]
# declare a cursor object from the connection
conn = connect(conn_params_dic)
cursor = conn.cursor()
# Execute query 
sql = "SELECT MAX(time_stamp_nyc) FROM [YOUR DAILY TABLE]"
cursor.execute(sql)

# Fetch the latest trading date
latest_daily_candle = cursor.fetchall()[0][0]  ## e.g. 2022-03-07

# Get the latest trading date from EUR_USD
now = datetime.now()
# set time to 17:00, otherwise it will pull the records that trade after 5PM today. 
now=now.replace(hour=17, minute=0, second=0, microsecond=0)
now_240_hrs = now - timedelta(hours=240)
# Convert to unix timestamp
dt0_now_240_hrs = int(now_240_hrs.timestamp())
dt1_now = int(now.timestamp())
interval = 'D'
df_r = get_forex_candles('OANDA:EUR_USD', interval, dt0_now_240_hrs, dt1_now)
latest_trading_date = df_r['ts'].max()  ## e.g. 2022-03-08

"""
Logic Test:

Example 1
Suppose today is Monday at 9PM, code is running on daily basis.
Then 
- now = Monday at 5PM
- latest_trading_date = Last Sunday at 5PM, note this is the market opening time from Sunday to Monday
- latest_daily_candle = Last Thursday at 5PM, note this is the latest market opening time in database
In this case latest_trading_date > latest_daily_candle, then incremental load is triggered
- date0 = Last Friday at 5PM, by applying the transformation below
- date1 = Monday at 5PM, by applying the transformation below
By running the incremental load, it populates the data from Sunday 5PM to Monday 5PM -> OK!


Example 2
Suppose today is Tuesday at 9PM, code is running on daily basis.
Then 
- now = Tueday at 5PM
- latest_trading_date = Monday at 5PM, note this is the market opening time from Monday to Tuesday
- latest_daily_candle = Last Sunday at 5PM, note this is the latest market opening time in database
In this case latest_trading_date > latest_daily_candle, then incremental load is triggered
- date0 = Monday at 5PM, by applying the transformation below
- date1 = Tuesday at 5PM, by applying the transformation below
By running the incremental load, it populates the data from Monday 5PM to Tuesday 5PM -> OK!


Example 3
Suppose today is Friday at 9PM, code is running on daily basis.
Then 
- now = Friday at 5PM
- latest_trading_date = Thursday at 5PM, note this is the market opening time from Thursday to Friday
- latest_daily_candle = Wednesday at 5PM, note this is the latest market opening time in database
In this case latest_trading_date > latest_daily_candle, then incremental load is triggered
- date0 = Thursday at 5PM, by applying the transformation below
- date1 = Friday at 5PM, by applying the transformation below
By running the incremental load, it populates the data from Thursday 5PM to Friday 5PM -> OK!


Example 4
Suppose today is Saturday at 9PM, code is running on daily basis.
Then 
- now = Saturday at 5PM
- latest_trading_date = Thursday at 5PM, note this is the market opening time from Thursday to Friday
- latest_daily_candle = Thursday at 5PM, note this is the latest market opening time in database
In this case latest_trading_date = latest_daily_candle, then incremental load is NOT triggered -> OK!


Example 5
Suppose today is Sunday at 9PM, code is running on daily basis.
Then 
- now = Sunday at 5PM
- latest_trading_date = Thursday at 5PM, note this is the market opening time from Thursday to Friday
- latest_daily_candle = Thursday at 5PM, note this is the latest market opening time in database
In this case latest_trading_date = latest_daily_candle, then incremental load is NOT triggered -> OK!

"""


# If-else condition - If the latest trading date (fully traded) is later than the max date in database,
# then data is not up to date. We need to load the data.

# ==============================================Load daily candle data================================================
start = time.time()

date0 = latest_daily_candle + timedelta(hours=24)
date1 = latest_trading_date + timedelta(hours=24)

dt0 = int(date0.timestamp())
dt1 = int(date1.timestamp())
interval = 'D'

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

            # Run the copy_from_dataFile method, here saving data into [YOUR DAILY TABLE]
            copy_from_dataFile_daily(conn, df_r, '[YOUR DAILY TABLE]')

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
msg = f"The ETL job for refreshing [YOUR DAILY TABLE] table is completed on {dt.date.today().strftime('%Y/%m/%d')}. \n" \
      f"Sending at {dt.datetime.now().strftime('%H:%M:%S.%f')}," \
      f" and elapsed time is {Elapsed_Time}. \n" \
      f"Update Message: {body} "

if __name__ == '__main__':
    email_alert("Algo Trading ETL process update", msg, "TEST1@gmail.com; TEST2@gmail.com")
