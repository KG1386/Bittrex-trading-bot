'''
Created on 11 Sep 2017

@author: home
'''
import psycopg2
import config
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import numpy as np
from datetime import datetime

''' Function that checks if database connection can be established and if so, 
# print out the db version '''
def db_check():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config.db_config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
 
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
        # close the communication with the PostgreSQL
        cur.close()
        # Assume that database is OK 
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
 
 
''' Used to add new row to the table'''
def new_entry(DATE_K, TIME_K, BAL_VOLUME, VOLUME, OPEN, HIGH, LOW, CLOSE, currency):
    conn = None
    try:
        params = config.db_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
    
        query =  "INSERT INTO " + currency + " (DATE_K, TIME_K, BAL_VOLUME, VOLUME, OPEN, HIGH, LOW, CLOSE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        data = (DATE_K, TIME_K, BAL_VOLUME, VOLUME, OPEN, HIGH, LOW, CLOSE)

        cur.execute(query, data)
        #
        #cur.execute(str_to_record);
        
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        # print ("Records in ", currency, " created successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

''' Universal function for inserting given content (list of lists (column name, value)) into any 
already created table table'''
def db_create_row (table, content_list):
    data = []
    conn = None
    try:
        params = config.db_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        query =  "INSERT INTO " + table + " ("
        for idx, element in enumerate(content_list):
            # If this is not the last element in the array, add comma for the next element
            if (idx + 1) < len(content_list):
                query = query + " " + element[0] + ","
            else:
                query = query + " " + element[0]
                 
        query = query + ") VALUES ("
         
        # Add enough placeholders for each table value
        for idx, element in enumerate(content_list):
            if (idx + 1) < len(content_list):
                query = query + "%s, "
            else:
                query = query + "%s);"
#         
        # Save values of the table's elements:
        for idx, element in enumerate(content_list):
            data.append(element[1])
        
        
        #query =  "INSERT INTO " + table + " (DATE_K, TIME_K, BAL_VOLUME, VOLUME, OPEN, HIGH, LOW, CLOSE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        #data = (DATE_K, TIME_K, BAL_VOLUME, VOLUME, OPEN, HIGH, LOW, CLOSE)
        
        cur.execute(query, data)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        # print ("Records in ", currency, " created successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
'''Universal row reader given that column key name and key is given''' 
def db_read_row (table, rows, key_column, key):
    conn = None
    try:
        params = config.db_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        #cur.execute("SELECT * FROM " + table + " WHERE (" + key_column + " = " + key + ")")
        query = "SELECT * FROM " + table + " WHERE " + key_column + " = %s"
        cur.execute(query, (key))
        #cur.execute("SELECT FROM " + table + " WHERE " + key_column + " = %s", (key))
        if cur.fetchone() is not None:
            row = cur.fetchone()
            return row
        else:
            return None
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

'''Universal row remover based on a table key'''
def db_remove_row(table, key_column, key):
    conn = None
    try:
        params = config.db_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        cur.execute("DELETE FROM " + table + " WHERE " + key_column + " = %s", (key))
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
'''Can be used to query only a selected set of rows in table'''
def get_rows(com_str):
    conn = None
    try:
        params = config.db_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        #cur.execute("SELECT DATE_K, TIME_K, BAL_VOLUME, VOLUME, OPEN, HIGH, LOW, CLOSE FROM " + table + " ORDER BY DATE_K, TIME_K ")
        cur.execute(com_str)
        rows = cur.fetchall()
        print("The number of parts: ", cur.rowcount)
        for row in rows:
            print(row)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
      
'''Deletes table and all contents in a given database'''
def flush_table(table_to_flush):
    conn = None
    try:
        params = config.db_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        com_str = "DROP TABLE " + table_to_flush + ";"
        cur.execute(com_str)      
        
        cur.close()
        # commit the changes
        conn.commit()
        print ("table deleted")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
''' Connect to default database in order to create new database'''
def new_db():
    conn = None
    try:
        conn = psycopg2.connect(database = "postgres", user = "home", password = "8375444", host = "localhost", port = "5432")
        print ("Opened database successfully")
        
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        # Create new database
        cur.execute('CREATE DATABASE ' + 'TRADE_DB')
        print ("db_created")
        cur.close()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
'''Should be called right after creating the new database or clearing the old one, 
creates table by default and records current 10 days worth of data into it'''
def new_table(com_str):
    conn = None
    try:
        params = config.db_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # Create table with given name if it doesnt exists yet
        cur.execute(com_str)
 
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        # print (table_name, " table created/existence verified successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        # print ("Failed to create table for ", table_name)
        print(error)
    finally:
        if conn is not None:
            conn.close()


''' Outputs all records''' 
def read_table(currency,period):
    conn = None
    market_data_arr = np.zeros(shape = (period, 6))
    date_arr = np.zeros(shape = (period, 1), dtype='datetime64[m]')
    
    try:
        params = config.db_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        cur.execute("SELECT DATE_K, TIME_K, BAL_VOLUME, VOLUME, OPEN, HIGH, LOW, CLOSE from " + currency)
        rows = cur.fetchall()
        # Extract only data within specified time 
        for idx, row in enumerate(rows[-period:]):
            comb_date = datetime.combine((row[0]), (row[1]))
            date_arr[idx] = np.datetime64(comb_date) # DATE
            
            market_data_arr[idx,0] = row[2] # BAL_VOLUME
            market_data_arr[idx,1] = row[3] # VOLUME
            market_data_arr[idx,2] = row[4] # OPEN 
            market_data_arr[idx,3] = row[5] # HIGH
            market_data_arr[idx,4] = row[6] # LOW
            market_data_arr[idx,5] = row[7] # CLOSE
#         
        print ("Required records successfully retrieved")
        conn.close()
        return date_arr, market_data_arr
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
          
''' Obtain the very last entry in the DB by sorting the table by date and time and 
picking the last/first entry (defined by parameter sent to function)'''
def entry_check(entry_pos, currency_table):
    conn = None
    try:
        params = config.db_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT (DATE_K), TIME_K  FROM " + currency_table + " ORDER BY DATE_K, TIME_K ")
        rows = cur.fetchall()
        # Check if there is any content in the table
        if len(rows) != 0:
            if entry_pos == 'first':
                last_entry = (rows[0])
            
            else:
                last_entry = (rows[-1])
                
            date_str = (str(last_entry[0]) + " " + str(last_entry[1]))
            #date_str = last_entry[0] + " " + last_entry[1]
            print (date_str)
            
            cur.close()
                
            return date_str
        else:
            return False
            
    except (Exception, psycopg2.DatabaseError) as error:
        # Return false, indicating most likely that the table does not exist
        return False
        print(error)
    finally:
        if conn is not None:
            conn.close()
