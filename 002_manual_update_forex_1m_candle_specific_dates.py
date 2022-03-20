

# ===============================================Call Packages======================================================
import os
import time
import pandas as pd
import datetime as dt
from datetime import datetime

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


# ==============================================Define the date ranges================================================

# set the target date range
start_date_time_str = '01/02/22 00:00:00'
start_date_time = datetime.strptime(start_date_time_str, '%d/%m/%y %H:%M:%S')

end_date_time_str = '17/03/22 00:00:00'
end_date_time = datetime.strptime(end_date_time_str, '%d/%m/%y %H:%M:%S')

# define the date difference as integer
numdays = (end_date_time - start_date_time).days + 1

# define the empty lists
dateList = []
dateList_unix = []

# get a list of dates
for x in range (0, numdays):
    dateList.append(start_date_time + dt.timedelta(days = x))

# convert the dates to unix timestamp
for x in dateList:
    dateList_unix.append(int(x.timestamp()))



# =============================================Load 1-m candle data================================================
start = time.time()
interval = '1'

for dt0, dt1 in zip(dateList_unix, dateList_unix[1:]):
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
            tmp_df = "df_temp_1m.csv"
            os.remove(tmp_df)

        except Exception:
            continue

end = time.time()

Elapsed_Time = (end - start)
print('Time Elapsed: ', end - start)



# ==========================================Send Email Alert Notification===================================================
body = f"The ETL job for refreshing forex_precious_metal_1m_jason_stage table is completed on {dt.date.today().strftime('%Y/%m/%d')}," \
       f" sending at {dt.datetime.now().strftime('%H:%M:%S.%f')} ," \
       f" and elapsed time is {Elapsed_Time} "

if __name__ == '__main__':
    email_alert("Algo Trading ETL process update", body, "emailalertjasonlu900625@gmail.com ")

