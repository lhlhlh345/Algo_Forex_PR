
# Public Release - Forex and Precious Metal Price and Volume ETL process
### Author: Jason (Hao) Lu
### Date: Dec 13th, 2022


## ETL Workflow Description: 
- The purpose of this ETL process is pulling and updating the daily and 1-minute candles for selected Forex and Precious Metal symbols. The steps are described as following:
    1. Run "001_update_forex_symbol_list.py" to update the selected symbols.
    2. Run "001_manual_update_forex_1m_candle_ALL.py" and "001_manual_update_forex_daily_candle_ALL.py" to initiate the first-time data loading. Note that due to large volume of 1-min records, it takes days to load past 10-year data.
    3. If the data loading goes wrong, "002_manual_update_forex_1m_candle_specific_dates.py" is readily available to pull 1-min candles for specific dates. 
    4. "003_incremental_load_forex_1m_candle.py" and "003_incremental_load_forex_daily_candle.py" can be used to run daily scheduled ETL processes. Suggested running time window is 20:15 - 11:45.
## File Description:
- utils:
    - database_util.py: defining the functions of 
        - connecting postgresql database
        - showing postgresql exceptions
        - truncating database table
        - data loading method of copy_from
        - data loading method of execute_many
    - finnhub_util.py: defining the functions of 
        - get_forex_symbols
        - get_forex_candles
    - notification_util.py: defining the functions of
        - email_alert
        - SMS_alert

- "etl_init.py" is used to fetch data of finnhub, database, and alert credentials from ".env" file. Please save your credentials to ".env" file.

