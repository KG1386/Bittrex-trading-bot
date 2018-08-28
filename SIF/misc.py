'''
Created on 11 Sep 2017

@author: home
'''
# Some essential python libraries
import re
import sys # for serious error handling

# Financial library with large number of indicators
import talib
# File responsible for PostgreSQL database connection and data manipulation
from SIF import db_access

# Find the time difference between latest db entry and newest entry
from datetime import datetime
import time
fmt = '%Y-%m-%d %H:%M:%S'


###################################################################
# Bittrex python wrapper and necessary parameters to establish connection

from bittrex_v2 import Bittrex
b_1 = Bittrex(api_key="f4db35970a6f459298e2bb59977f9edc", api_secret="588fd5526c9d4ea7bc09909f93563fc9")

import bittrex.bittrex as bt
b_2 = bt.Bittrex("f4db35970a6f459298e2bb59977f9edc", "588fd5526c9d4ea7bc09909f93563fc9")
###################################################################

# Slice dictionary by the entry in it
def sliceDict(d, s):
    return {k:v for k,v in d.items() if k.startswith(s)}

# Function that can extract digits out of string, even when digits are in scientific form
def extractNums(string):
    
    match_number = re.compile('-?\ *[0-9]+\.?[0-9]*(?:[Ee]\ *-?\ *[0-9]+)?')
    final_list = [float(x) for x in re.findall(match_number, string)]
    
    if (final_list) == []:
        final_list = 0
    else:
        final_list = float(final_list[0])
        
    return final_list

# Performs basic format cleaning of the data from the exchange, until better way is found
def clearData(entry):
    
    # Obtain each entry and record it in the database
    date_t = (sliceDict(entry, 'T')).values()
    for time_t in date_t:
        date_t, time_t = time_t.split("T")[0:]
    # Create postgre type of date record
    #date = date + " " + time
     
    # Basic price indicators
    bal_volume = str((sliceDict(entry, 'BV')).values())
    bal_volume = extractNums(bal_volume)
    
    volume = str((sliceDict(entry, 'V')).values())
    volume = extractNums(volume)
    
    open_c = str((sliceDict(entry, 'O')).values())
    open_c = extractNums(open_c)
        
    high = str((sliceDict(entry, 'H')).values())
    high = extractNums(high)
        
    low = str((sliceDict(entry, 'L')).values())
    low = extractNums(low)
        
    close = str((sliceDict(entry, 'C')).values())
    close = extractNums(close)
    
    #print (type(date), type(time), type(average))
    # print (date_t, time_t, bal_volume, open_c, high, low, close)
        
    return date_t, time_t,  bal_volume, volume, open_c, high, low, close

''' Table annihilation function in case global changes are needed'''
def removeTables(curr_pairs):
    for i in curr_pairs:
        db_access.flush_table(i)
        
        
# Performs initial and subsequent database update from the exchange based on latest data
def recordToDB(date_str, market_data, currency):
    # Check if currency table exists
    last_db_entry = db_access.entry_check('last', currency)
    print (currency, "last entry", last_db_entry)
    
    if last_db_entry == date_str and date_str != 0:
        print ("Data in DB is up to date")
        return True
    elif date_str == 0:
        
        # Save whole 10 days obtained to the table
        for i in market_data:
            date_t, time_t, bal_volume, volume, open_c, high, low, close = clearData(i)
            indicator_list = (("DATE_K",date_t),("TIME_K", time_t),("BAL_VOLUME", bal_volume),("VOLUME", volume),("OPEN", open_c),
                          ("HIGH", high),("LOW", low), ("CLOSE",close))
            db_access.db_create_row(currency, indicator_list)
    else:
        print ("Updating the data in DB...")
        d1 = datetime.strptime(date_str, fmt) # Latest Bittrex data
        d2 = datetime.strptime(last_db_entry, fmt) # Latest data saved in db 
        # Convert to common format
        d1_ts = time.mktime(d1.timetuple())
        d2_ts = time.mktime(d2.timetuple())
        
        # Find the difference and convert to minutes
        minDiff = int((d1_ts-d2_ts) / 60)
        # Use difference in timestamps to update only missing records (oldest saved - newest obtained)
        for i in market_data[-minDiff:]:
            date_t, time_t, bal_volume, volume, open_c, high, low, close = clearData(i)
            indicator_list = (("DATE_K",date_t),("TIME_K", time_t),("BAL_VOLUME", bal_volume),("VOLUME", volume),("OPEN", open_c),
                          ("HIGH", high),("LOW", low), ("CLOSE",close))
            db_access.db_create_row(currency, indicator_list)
        print ("Database update finished, last date: ", d1, "in ", currency)
        
        
        
# Performs initial update on database tables for specific currencies
def updateDB(currencies):
    # Iterate over a list of currencies and create/update the table for it
    for currency in currencies:
        last_entry_res = db_access.entry_check('last', currency)
        # 10 retries are given to try to reconnect to database
        for err in range(10):
        # Attempt to retrieve data from exchange
            try:
                market_data = b_1.get_ticks((currency[0:3] + "-" + currency[4:]), 'oneMin')
                if market_data is not None:
                    break
            except:
                print ("Connection to exchange failed, retrying for ", err, "times")
                time.sleep(5)
                err += 1
                # If number of retries reaches the limit, stop the program
                if err >= 10: 
                    sys.exit("Connection to exchange failed")
                
        market_data = market_data['result']
            
        if last_entry_res != False:
            print (currency, " currency table is OK")
            #date, time_t, bal_volume, volume, open_c, high, low, close = clearData(market_data[-1])
            data_out = clearData(market_data[-1])
            date_str = data_out[0] + " " + data_out[1]
            recordToDB(date_str, market_data, currency) 
         
        elif last_entry_res == False:
            print ("Database is empty, filling in...")
            recordToDB(0, market_data, currency)
        
     
# Function to read selected table from the database, record the result in numpy array...
# ...and calculate specified financial function'''    
def getIndicators(currency, period, ind_length):
    # 2D array of 5 market indicators
    date_arr, market = db_access.read_table(currency, period)
    date_t = (date_arr[:,0].flatten())
    #date_t = np.array([[date_t,date_t],[date_t,date_t]],dtype='M8[m]').astype('O')[0,1]
    
    close_price = market[:,5].flatten()
    volume = market[:,1].flatten()
    high = market[:,3].flatten()
    low = market[:,4].flatten()
    
    rsi_long = talib.RSI(close_price, ind_length['rsi_l']) # @UndefinedVariable
    rsi_short = talib.RSI(close_price, ind_length['rsi_s']) # @UndefinedVariable   
    tema_result = talib.TEMA(close_price, 30) # @UndefinedVariable
    on_balance = talib.OBV(close_price, volume) # @UndefinedVariable
    # will_r = talib.WILLR(high, low, close_price, timeperiod=1440) # @UndefinedVariable
    #will_r = talib.LINEARREG_ANGLE(close_price, timeperiod=ind_length['reg']) # @UndefinedVariable
    
    return date_arr, close_price, rsi_long, rsi_short, tema_result, on_balance

# Normalise indicators range to positive/negative 1
def normaliseIndicators(data_out):
    norm_data = []
    # Start from element 2 as element 0 is always the date and element 1 is the closing price
    norm_data.append((data_out[2][-1] - 50)/50) # RSI Long
    norm_data.append((data_out[3][-1] - 50)/50) # RSI Short
    norm_data.append((data_out[1][-1] - data_out[4][-1])/data_out[4][-1]) # Average
    
    if data_out[5][-1] > 0:
        norm_data.append((data_out[5][-1] - max(data_out[5]))/max(data_out[5]))
    else:
        norm_data.append((data_out[5][-1] + min(data_out[5]))/max(data_out[5]))
    
    return norm_data


def formatMrktSum(currency, paramChoice):
    for err in range(10):
        # Attempt to retrieve data from exchange
        try:
            result = b_1.get_market_summary((currency[0:3] + "-" + currency[4:]))
            
            if result is not None:
                break
        except:
            print ("Connection to exchange failed, retrying for ", err, "times")
            time.sleep(5)
            err += 1
            # If number of retries reaches the limit, stop the program
            if err >= 10: 
                return None
                        
    result = result['result']
    if result != None:
        # Get available coins, not tied in orders
        result = str((sliceDict(result, 'Ask')).values())
        result = extractNums(result)
        if result == 0:
            print (result)
    return result


def formatBalance(currency, paramChoice):
    for err in range(10):
        # Attempt to retrieve data from exchange
        try:
            result = b_1.get_balance(currency)
            
            if result is not None:
                break
        except:
            print ("Connection to exchange failed, retrying for ", err, "times")
            time.sleep(5)
            err += 1
            # If number of retries reaches the limit, stop the program
            if err >= 10: 
                return None
                        
    result = result['result']
    if result != None:
        # Get available coins, not tied in orders
        if paramChoice == 0:
            result = str((sliceDict(result, 'Available')).values())
            result = extractNums(result)
        # Get the total amount of coins, including the ones that are tied in orders
        elif paramChoice == 1:
            result = str((sliceDict(result, 'Balance')).values())
            result = extractNums(result)
    return result

def formatOrderBook(currency):
    result = b_2.get_orderbook((currency), 'sell', 1)['result']
    
    amount = str((sliceDict(result[0], 'Quantity')).values())
    amount = extractNums(amount)
    
    rate = str((sliceDict(result[0], 'Rate')).values())
    rate = extractNums(rate)
    
    return amount, rate

def getBuySignal(close, average, RSI, latestSell):
    # Check if RSI is sufficiently low and money is available to buy
    # New buy should also be a certain percentage lower than the last sell, to avoid buying at the tops
    if RSI < 40 and close < latestSell and close < average:
        return True
    else:
        return False
    
