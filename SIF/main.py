'''
Created on 30 Sep 2017

@author: home
'''

''' 
Simple Indicator Follower (SIF) Module that provides basic trading based on some relatively concrete indicator signs such as:
 - RSI approaching or below 30%
 - Volume is stable 
 In such case bot can commit to buy order
If there is a buy order and the price is above the buy price by some percentage, sell
'''
import os
import sys

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

# Miscellaneous functions in here
from SIF import db_access, misc

import time

# Useful plotting functions:

# Bittrex python wrapper and necessary parameters to establish connection
from bittrex_v2 import Bittrex
b_1 = Bittrex(api_key="key", api_secret="secret")

import bittrex.bittrex as bt
b_2 = bt.Bittrex("key", "secret")
#############################################################################################
# Zcash, Waves, Etherium, NEO, ark, omisego, litecoin, lisk, ripple, monero to BTC
curr_pairs = ("BTC_ZEC","BTC_WAVES", "BTC_ETH", "BTC_NEO", "BTC_ARK", "BTC_OMG", "BTC_LTC", "BTC_LSK", "BTC_XRP", "BTC_XMR")
# Retrieve balance and find how much can be given for each coin (equal amount for now)
curDiv = misc.formatBalance('BTC', 0) / len(curr_pairs)
latestSell = 10000000 # Arbitrarily large value to ensure successful first sale
# Array to store parameters associated with each currency: 
# [1] - price that the currency has been last sold for
# [2] - B or S indicating Bought or Sold respectively
storeMoneyList = [['ZEC',latestSell,'S'], ['WAVES', latestSell, 'S'], ['ETH',latestSell, 'S'], ['NEO',latestSell, 'S'], ['ARK',latestSell, 'S'],
                   ['OMG',latestSell, 'S'], ['LTC',latestSell, 'S'], ['LSK',latestSell, 'S'], ['XRP',latestSell,'S'], ['XMR', latestSell, 'S']]
# Dictionary for storing period parameter of technical indicators
# rsi long, rsi short, kama indicator, linear regression
ind_length = {'rsi_l': 720,'rsi_s': 5,'tema': 1440, 'reg': 120}

def_period = 1440 # 0.5 days, since data is saved in minutes resolution
minOrderSize = 0.0005 # Lowest order size defined by exchange
gain = 0.003 # Percentage gain per trade
balanceToTrade = 0.001 # Allowed balance to trade

if __name__ == '__main__':
    # Check database condition
    db_access.db_check()
    # Verify that tables for storing transactions and currency prices exist and create it, if not
    db_access.new_table("CREATE TABLE IF NOT EXISTS TRANSACTIONS (ID TEXT NOT NULL, CURRENCY TEXT NOT NULL, "\
    "PRICE REAL NOT NULL, AMOUNT REAL NOT NULL, PRIMARY KEY(ID));")
    # Create new table, since 0 records likely indicate table does not exist yet 
    for currency in curr_pairs: 
        db_access.new_table("CREATE TABLE IF NOT EXISTS " + currency +
    " (DATE_K DATE NOT NULL, TIME_K TIME NOT NULL, BAL_VOLUME REAL NOT NULL, VOLUME REAL NOT NULL, OPEN REAL NOT NULL, HIGH REAL NOT NULL, "\
     "LOW REAL NOT NULL, CLOSE REAL NOT NULL, PRIMARY KEY(DATE_K, TIME_K));")
        
    ''' Update all tables with the latest data from the exchange ''' 
    misc.updateDB(curr_pairs)
        

counter = 0
try:
    while True:
        counter = counter + 1
        # Continue only if there is enough balance to buy anything (saves some power)
        if misc.formatBalance('BTC', 0) > minOrderSize:
            for idx, currency in enumerate(curr_pairs):
                time_period = max(ind_length.values()) + def_period
                data_out = misc.getIndicators(currency, time_period, ind_length)
                 
                totBalance = misc.formatBalance(currency[4:], 0)
                
                # Check if currency has been bought last time and that the currency balance is below threshold
                if storeMoneyList[idx][2] == 'B' and misc.formatBalance(currency[4:], 1) < minOrderSize:
                    storeMoneyList[idx][2] = 'S'
                    print ('Sold: ', currency)
                elif totBalance > minOrderSize:
                    b_2.sell_limit((currency[0:3] + "-" + currency[4:]), totBalance, abs(storeMoneyList[idx][1]))
                    
                # If coin has been sold last time and there is enough funds to buy some, continue
                if storeMoneyList[idx][2] == 'S':
                    #latestAsk = misc.formatMrktSum(currency, 0) # Obtain latest price
                    
                    amount, latestAsk = misc.formatOrderBook((currency[0:3] + "-" + currency[4:]))
                    minAllowedOrder = (balanceToTrade / latestAsk)
                    if minAllowedOrder > amount:
                        minAllowedOrder = amount
                    #amount = (latestAsk * curDiv * gain) + (latestAsk * curDiv)
                    buySignal = misc.getBuySignal(latestAsk, data_out[4][-1], data_out[3][-1], abs(storeMoneyList[idx][1]))
                    
                    if buySignal == True:
                        b_2.buy_limit((currency[0:3] + "-" + currency[4:]), minAllowedOrder, latestAsk)
                        time.sleep(2) # Wait for 1s to ensure that the order is fullfilled
        
                        sellPrice = (latestAsk * gain) + latestAsk # Find the actual sell price with small percentage for gain + tax
                        storeMoneyList[idx][1] = -sellPrice # Store latest expected sell price for the next round (negative to indicate sell)
                        # Verify that the amount that is larger than minimum order size has been bought
                        if totBalance > minOrderSize:
                            print ('Bought: ', currency, 'At: ', latestAsk, 'Amount:', minAllowedOrder)
                            storeMoneyList[idx][2] = 'B'
                            b_2.sell_limit((currency[0:3] + "-" + currency[4:]), totBalance, sellPrice)
                            print ('Put sell order for: ', currency, 'At: ', sellPrice, 'Amount:', totBalance)
        
            if counter == 10:
                # Approximately every minute update database
                misc.updateDB(curr_pairs)
                counter = 0
        
except KeyboardInterrupt:
    pass

