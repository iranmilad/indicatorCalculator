import pandas as pd
import ta
import redis
import json
import threading
import time
import queue
import requests
import sys, os
import _thread
import mysql.connector
from datetime import datetime

class IndicatorUpdate(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.host='localhost'
        self.port='6389'
        self.password=None
        self.host_price_server='149.28.120.38'
        self.port_price_server='6379'
        self.password_price_server='6$gtA453'
        self.mysqluser='root'
        self.mySqlHost='localhost'
        self.mySqlDBName='screener'
        self.forex_symbol=self.getExchangeSymbols('IB')
        self.crypto_symbol=self.getExchangeSymbols('Binance')
        self.primary_nasdaq=self.getExchangeSymbols('Nasdaq')
        self.primary_nyse=self.getExchangeSymbols('Nyse')
        #self.q=q

    # def run(self):
    #     f, args =self.q.get()
    #     f(*args)
    #     self.q.task_done()


    def get_database_number(self,interval):
            return 14
          
    def connect_to_mysql(self):
        # Connecting from the server
        conn = mysql.connector.connect(user = 'root',
                                    host = 'localhost',
                                    database = 'screener')
        return conn

    def calculate_indicator(self,symbols,interval):
        start_time = time.time()
        database=self.get_database_number(interval)

        r = redis.Redis(host=self.host_price_server, port=self.port_price_server ,password =self.password_price_server , db=database)
        symbols_return={"crypto":[],"forex":[],"stock":[]}


        for symbol in symbols:

            try:
                if not r.exists(symbol):
                    print("symbol is not exist in %s "%(symbol))
                    continue
                price_data=r.get(symbol)

                if price_data:
                    series_buffer=json.loads(price_data)[0]
                    #print(series_buffer)
                else:
                    print ("key Symbol %s not string in timeframe %s" %(symbol,interval))
                    print ("import is  %s with type %s" %(price_data,type(price_data)))
                    continue
            except:
                print("connection problem to get symbol %s"%(symbol))
                continue


            if len(series_buffer)==0:
                print ("Symbol %s is null in timeframe %s" %(symbol,interval))
                continue

            if symbol in self.crypto_symbol:
                symbols_return["crypto"].append({
                    "symbol" : symbol,
                    "current_price": series_buffer['close'],
                    "previous_price" : series_buffer['prevClose'],
                    "price_change": round(((series_buffer['close']-series_buffer['prevClose'])/series_buffer['prevClose'])*100,1) ,
                    "volume" : series_buffer['volume'],
                })
            elif symbol in self.forex_symbol:
                symbols_return["forex"].append({
                    "symbol" : symbol,
                    "current_price": series_buffer['close'],
                    "previous_price" : series_buffer['prevClose'],
                    "price_change": round(((series_buffer['close']-series_buffer['prevClose'])/series_buffer['prevClose'])*100 ,3),
                    "volume" : series_buffer['volume'],
                })
            else:
                symbols_return["stock"].append({
                    "symbol" : symbol,
                    "current_price": series_buffer['close'],
                    "previous_price" : series_buffer['prevClose'],
                    "price_change": round(((series_buffer['close']-series_buffer['prevClose'])/series_buffer['prevClose'])*100,1) ,
                    "volume" : series_buffer['volume'],
                })

        if symbols_return:

            self.update_key(symbols_return)
            print("%s updated" %symbol)
        else:
            print('symbols is none')

        r.connection_pool.disconnect()
        

    def getSymbols(self):
        url = 'https://panel.scanical.com/api/symbols/snapshot'
        try:
            x = requests.get(url)
            responses = x.json()
            # ...
        except ValueError:
            responses = None

        except:
            print("Connection refused")
            responses = None

        return responses

    def load_machines(self):

        symbols=self.getSymbols()

        self.calculate_indicator(symbols,"1d")

    def getExchangeSymbols(self,exchange):
        url = 'https://panel.scanical.com/api/exchangesymbols/'+exchange
        try:
            x = requests.get(url)
            return x.json()
        except:
            return []
  
    def update_key(self,symbols_return):
        sql=''
        mydb = mysql.connector.connect(user = self.mysqluser, host = self.mySqlHost, database = self.mySqlDBName)

        mycursor = mydb.cursor()


        for symbol in symbols_return["crypto"]:
            sql = "UPDATE `cryptos` SET `volume`=%s,`previous_price`=%s,`current_price`=%s,`price_change`=%s WHERE `symbol`=%s;"
            val = (
                float(symbol['volume']),
                float(symbol['previous_price']),
                float(symbol['current_price']),
                float(symbol['price_change']),                                             
                symbol['symbol']
            )
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "details inserted")



        for symbol in symbols_return["forex"]:
            sql = "UPDATE `currencies` SET `volume`=%s,`previous_price`=%s,`current_price`=%s,`price_change`=%s WHERE `pair`=%s;"
            val = (
                float(symbol['volume']),
                float(symbol['previous_price']),
                float(symbol['current_price']),
                float(symbol['price_change']),                                             
                symbol['symbol']
            )
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "details inserted")


        for symbol in symbols_return["stock"]:
            sql = "UPDATE `stocks` SET `volume`=%s,`previous_price`=%s,`current_price`=%s,`price_change`=%s WHERE `symbol`=%s;"
            val = (
                float(symbol['volume']),
                float(symbol['previous_price']),
                float(symbol['current_price']),
                float(symbol['price_change']),                                             
                symbol['symbol']
            )
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "details inserted")

        mydb.close()


print('run service')


q=queue.Queue()



if __name__ == '__main__':

    sleep_loop_time=90   
    indicator=IndicatorUpdate()

    while True:
        
        start_time = time.time()

        try:
            indicator.load_machines()
        except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
        

        print("%s --- %s seconds ---" % (datetime.now(),time.time() - start_time))
        
        if sleep_loop_time-(time.time() - start_time)>0:
            time.sleep(sleep_loop_time-(time.time() - start_time))

