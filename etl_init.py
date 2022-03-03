import os
from dotenv import load_dotenv

load_dotenv()

FINNHUB_CONFIG = {
                  "finnhub_api_key_1": os.getenv('api1'),
                  "finnhub_api_key_2": os.getenv('api2'),
                  "finnhub_api_key_3": os.getenv('api3'),
                  "finnhub_api_key_4": os.getenv('api4')
                 }

DB_CONFIG = {
                "db_host": os.getenv('host_local'),
                "db_port": os.getenv('port'),
                "db_name": os.getenv('DB_local'),
                "db_user": os.getenv('user'),
                "db_pass": os.getenv('password')
            }  

ALERT_CONFIG = {
                "emailACCT": os.getenv('emailACCT'),
                "emailPWD": os.getenv('emailPWD'),
                'SMS_account_sid': os.getenv('SMS_account_sid'),
                'SMS_auth_token': os.getenv('SMS_auth_token'),
                'SMS_source': os.getenv('SMS_source'),
                'SMS_destination': os.getenv('SMS_destination'),                
               }


