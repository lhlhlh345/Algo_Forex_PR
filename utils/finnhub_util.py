# ===============================================Call Packages======================================================
import finnhub
import pandas as pd
import numpy as np
import datetime as dt

from finnhub import FinnhubAPIException
from etl_init import FINNHUB_CONFIG

from utils.notification_util import email_alert


# ===============================================get_forex_symbols==================================================
## Using 1st Finnhub API KEY - used to load other Finnhub data
def get_forex_symbols():

    try:
        finnhub_client = finnhub.Client(api_key=FINNHUB_CONFIG["finnhub_api_key_2"])  ### note: finnhub_api_key_2 is associated with premium account
        res = finnhub_client.forex_symbols('OANDA')

    except FinnhubAPIException as err:
        body = f"Error occurs during calling Finnhub /forex/candle/profile API - {err}"
        print(body)
        email_alert("Algo Trading ETL process update", body, "emailalertjasonlu900625@gmail.com")

        return pd.DataFrame()

    symbols = pd.DataFrame(res)
    symbols.sort_values(by=["symbol"], ascending=True, inplace=True, ignore_index=True)
    return symbols



# ===============================================get_forex_candles===================================================

## Using 2nd Finnhub API KEY - used to load Finnhub daily candle data
def get_forex_candles_daily(symbol, interval, start, end):

    try:
        finnhub_client = finnhub.Client(api_key=FINNHUB_CONFIG["finnhub_api_key_2"]) ### note: finnhub_api_key_2 is associated with premium account
        res = finnhub_client.forex_candles(symbol, interval, start, end)
    except FinnhubAPIException as err:
        body = f"Error occurs during calling Finnhub /forex/candle/profile API - {err} for forex {symbol}"
        print(body)
        email_alert("Algo Trading ETL process update", body, "emailalertjasonlu900625@gmail.com")
        return pd.DataFrame()

    if res['s'] == 'no_data':
        body = f"No data is available for {symbol} between {start} and {end}"
        print(body)
        email_alert("Algo Trading ETL process update", body, "emailalertjasonlu900625@gmail.com")
        return pd.DataFrame()

    candles = pd.DataFrame(res)

    if interval == 'D':
        candles['ts'] = [dt.datetime.fromtimestamp(x) for x in candles['t']] #note: day candle returns GMT time at 12:00:00 AM
    else:
        candles['ts'] = [dt.datetime.fromtimestamp(x) for x in candles['t']] #note: 1 min candle returns NYC time
    
    candles['v'] = candles['v'].round().astype(int)
    x = np.char.replace(str(symbol), "['", '')
    t = np.char.replace(x, "']",'')
    candles['symbol'] = str(t)
  
    return candles



## Using 2nd Finnhub API KEY - used to load Finnhub 1-min candle data
def get_forex_candles_1m(symbol, interval, start, end):

    try:
        finnhub_client = finnhub.Client(api_key=FINNHUB_CONFIG["finnhub_api_key_2"])
        res = finnhub_client.forex_candles(symbol, interval, start, end)
    except FinnhubAPIException as err:
        body = f"Error occurs during calling Finnhub /forex/candle/profile API - {err} for forex {symbol}"
        print(body)
        email_alert("Algo Trading ETL process update", body, "emailalertjasonlu900625@gmail.com")
        return pd.DataFrame()

    if res['s'] == 'no_data':
        body = f"No data is available for {symbol} between {start} and {end}"
        print(body)
        email_alert("Algo Trading ETL process update", body, "emailalertjasonlu900625@gmail.com")
        return pd.DataFrame()

    candles = pd.DataFrame(res)

    if interval == 'D':
        candles['ts'] = [dt.datetime.utcfromtimestamp(x) for x in candles['t']] #note: day candle returns GMT time at 12:00:00 AM
    else:
        candles['ts'] = [dt.datetime.fromtimestamp(x) for x in candles['t']] #note: 1 min candle returns NYC time
    
    candles['v'] = candles['v'].round().astype(int)
    x = np.char.replace(str(symbol), "['", '')
    t = np.char.replace(x, "']",'')
    candles['symbol'] = str(t)
  
    return candles    


