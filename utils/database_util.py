# ===============================================Call Packages======================================================
import psycopg2
from psycopg2 import DatabaseError
from psycopg2 import OperationalError
import os
import sys

from utils.notification_util import email_alert



# ==============================================truncate_table=======================================================
def truncate_table(conn, table):

    truncate_sql = "TRUNCATE TABLE {0}".format(table)
    try:
        cursor = conn.cursor()
        cursor.execute(truncate_sql)
        conn.commit()
    except DatabaseError as error:
        print(f"Error:{error}")
        conn.rollback()
        cursor.close()
        raise DatabaseError



# ==============================================run_sql_file=========================================================
def run_sql_file(conn, sql_file):

    try:
        cursor = conn.cursor()
        cursor.execute(open(sql_file, "r").read())
        conn.commit()
    except DatabaseError as error:
        print(f"Error:{error}")
        conn.rollback()
        cursor.close()
        raise DatabaseError



# ==============================================show_psycopg2_exception==============================================
def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()    
    # get the line number when exception occured
    line_n = traceback.tb_lineno    
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)    
    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")
    


# ==============================================connect===============================================================
def connect(conn_params_dic):
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        print("Connection successfully..................")
        
    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)
        email_alert("Algo Trading ETL process update", show_psycopg2_exception(err), "emailalertjasonlu900625@gmail.com")        
        # set the connection to 'None' in case of error
        conn = None
    return conn


# ================================================copy_from_dataFile===================================================
# Used for general purpose
def copy_from_dataFile(conn, df, table):
#  Here we are going save the dataframe on disk as a csv file, 
#  load the csv file and use copy_from() to copy it to the table
    tmp_df = "df_temp.csv"
    df.to_csv(tmp_df, sep = "|", header=False,index=False)
    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep= "|")
        conn.commit()
        print("Data inserted using copy_from_datafile() successfully....")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        os.remove(tmp_df)
        # pass exception to function
        show_psycopg2_exception(error)
        email_alert("Algo Trading ETL process update", show_psycopg2_exception(error), "emailalertjasonlu900625@gmail.com")    
        cursor.close()



# Used for Daily Candle Load
def copy_from_dataFile_daily(conn, df, table):
#  Here we are going save the dataframe on disk as a csv file, 
#  load the csv file and use copy_from() to copy it to the table
    tmp_df = "df_temp_daily.csv"
    df.to_csv(tmp_df, sep = "|", header=False,index=False)
    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep= "|")
        conn.commit()
        print("Data inserted using copy_from_datafile() successfully....")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        os.remove(tmp_df)
        # pass exception to function
        show_psycopg2_exception(error)
        email_alert("Algo Trading ETL process update", show_psycopg2_exception(error), "emailalertjasonlu900625@gmail.com")    
        cursor.close()



# Used for 1-m Candle Load
def copy_from_dataFile_1m(conn, df, table):
#  Here we are going save the dataframe on disk as a csv file, 
#  load the csv file and use copy_from() to copy it to the table
    tmp_df = "df_temp_1m.csv"
    df.to_csv(tmp_df, sep = "|", header=False,index=False)
    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep= "|")
        conn.commit()
        print("Data inserted using copy_from_datafile() successfully....")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        os.remove(tmp_df)
        # pass exception to function
        show_psycopg2_exception(error)
        email_alert("Algo Trading ETL process update", show_psycopg2_exception(error), "emailalertjasonlu900625@gmail.com")    
        cursor.close()



# ================================================execute_many=======================================================


# used for general purpose
def execute_many(conn, datafrm, table, query):
    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]

    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))

    # SQL query to execute
    sql = query % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(sql, tpls)
        conn.commit()
        print("Data inserted using execute_many() successfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        email_alert("Algo Trading ETL process update", show_psycopg2_exception(err), "emailalertjasonlu900625@gmail.com")   
        cursor.close()