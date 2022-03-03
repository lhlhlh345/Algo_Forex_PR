
# General Packages
import os
import time
import pandas as pd
import datetime as dt


# Credential Parameters
from etl_init import DB_CONFIG


# Utility functions
from utils.database_util import connect
from utils.database_util import truncate_table
from utils.database_util import execute_many
from utils.finnhub_util import get_forex_symbols
from utils.notification_util import email_alert


#  Pull Stock Symbols and apply restrictions - We only consider common stocks traded on XNYS and XNAS
df_r = get_forex_symbols()

# Generate updated date time
df_r['Update_DateTime'] = pd.Timestamp.now()

# Generate the column "id" and move to front.
df_r['id'] = df_r.index
df_r = df_r[['id'] + [col for col in df_r.columns if col != 'id']]



# Load Database Parameters
conn_params_dic = {
    "host": DB_CONFIG['db_host'],
    "port": 5555,
    "database": DB_CONFIG['db_name'],
    "user": DB_CONFIG['db_user'],
    "password": DB_CONFIG['db_pass']
}

conn = connect(conn_params_dic)
conn.autocommit = True

# Start Time
start = time.time()

# Truncate the forex_precious_metal_symbol_jason_stage table
truncate_table(conn, 'forex_precious_metal_symbol_jason_stage')

# Run the execute_many method - Note that this controls the number of columns/fields in the database table
# Here I use execute_many() function because the amount of records is small
query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)"
        
execute_many(conn, df_r, 'forex_precious_metal_symbol_jason_stage', query)

# Close the connection
conn.close()

# End Time
end = time.time()

# Calculate Elapsed Time
Elapsed_Time = (end - start)
print(end - start)


# Send Email Alert Notification
body = f"The ETL job for refreshing forex_precious_metal_symbol_jason_stage table is completed on {dt.date.today().strftime('%Y/%m/%d')}," \
       f" sending at {dt.datetime.now().strftime('%H:%M:%S.%f')} ," \
       f" and elapsed time is {Elapsed_Time} "

if __name__ == '__main__':
    email_alert("Algo Trading ETL process update", body, "emailalertjasonlu900625@gmail.com ")



