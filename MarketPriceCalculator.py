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
import xmltodict
import math
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
        self.mySqlDBName='stockfeeder'
        # self.forex_symbol=self.getExchangeSymbols('IB')
        # self.crypto_symbol=self.getExchangeSymbols('Binance')
        # self.primary_nasdaq=self.getExchangeSymbols('Nasdaq')
        # self.primary_nyse=self.getExchangeSymbols('Nyse')
        #self.q=q

    # def run(self):
    #     f, args =self.q.get()
    #     f(*args)
    #     self.q.task_done()


    def get_database_number(self,interval):
            return 10
          
    def connect_to_mysql(self):
        # Connecting from the server
        conn = mysql.connector.connect(user = 'root',
                                    host = 'localhost',
                                    database = 'stockfeeder')
        return conn

    def calculate_indicator(self,symbols,patterns,interval):
        start_time = time.time()



        symbols_return={}

        for symbol in symbols:

            r = self.get_data(symbol)
            # try:
            if not symbol in r.keys():
                print("symbol is not exist in %s "%(symbol))
                continue
            price_data=r[symbol]
            series_buffer=price_data
            #     if price_data:
            #         series_buffer=json.loads(price_data)
            #         #print(series_buffer)
            #     else:
            #         print ("key Symbol %s not string in timeframe %s" %(symbol,interval))
            #         print ("import is  %s with type %s" %(price_data,type(price_data)))
            #         continue
            # except:
            #     print("connection problem to get symbol %s"%(symbol))
            #     continue


            if len(series_buffer)==0:
                print ("Symbol %s is null in timeframe %s" %(symbol,interval))
                continue

            series_buffer.reverse()
            df = pd.DataFrame(series_buffer[:-1])
            #df = ta.utils.dropna(df)
            previous_df=pd.DataFrame(series_buffer[:-2])
            if len(df)<=210 or len(previous_df)<=201:
                continue
            for pattern in patterns:

 
                #try:

                if pattern=="rate_of_return":
                    df['1']=(df.iloc[-1]["close"] - df.iloc[0]["close"]) / df.iloc[0]["close"]
                    df['5']=(df.iloc[-5]["close"] - df.iloc[0]["close"]) / df.iloc[0]["close"]
                    df['10']=(df.iloc[-10]["close"] - df.iloc[0]["close"]) / df.iloc[0]["close"]
                    df['20']= (df.iloc[-20]["close"] - df.iloc[0]["close"]) / df.iloc[0]["close"]
                    df['50']= (df.iloc[-50]["close"] - df.iloc[0]["close"]) / df.iloc[0]["close"]
                    df['100']= (df.iloc[-100]["close"] - df.iloc[0]["close"]) / df.iloc[0]["close"]
                    df['200']= (df.iloc[-200]["close"] - df.iloc[0]["close"]) / df.iloc[0]["close"]
                    

                    previous_df['1']= (previous_df.iloc[-1]["close"] - previous_df.iloc[0]["close"]) / previous_df.iloc[0]["close"]
                    previous_df['5']= (previous_df.iloc[-5]["close"] - previous_df.iloc[0]["close"]) / previous_df.iloc[0]["close"]
                    previous_df['10']= (previous_df.iloc[-10]["close"] - previous_df.iloc[0]["close"]) / previous_df.iloc[0]["close"]
                    previous_df['20']= (previous_df.iloc[-20]["close"] - previous_df.iloc[0]["close"]) / previous_df.iloc[0]["close"]
                    previous_df['50']= (previous_df.iloc[-50]["close"] - previous_df.iloc[0]["close"]) / previous_df.iloc[0]["close"]
                    previous_df['100']= (previous_df.iloc[-100]["close"] - previous_df.iloc[0]["close"]) / previous_df.iloc[0]["close"]
                    previous_df['200']= (previous_df.iloc[-200]["close"] - previous_df.iloc[0]["close"]) / previous_df.iloc[0]["close"]
                    
                    symbols_return[pattern]={
                            'value':{
                                '1':df.iloc[-1]['1'],
                                '5':df.iloc[-1]['5'],
                                '10':df.iloc[-1]['10'],
                                '20':df.iloc[-1]['20'],
                                '50':df.iloc[-1]['50'],
                                '100':df.iloc[-1]['100'],
                                '200':df.iloc[-1]['200']
                            },
                            'previous':{
                                '1':previous_df.iloc[-1]['1'],
                                '5':previous_df.iloc[-1]['5'],
                                '10':previous_df.iloc[-1]['10'],
                                '20':previous_df.iloc[-1]['20'],
                                '50':previous_df.iloc[-1]['50'],
                                '100':previous_df.iloc[-1]['100'],
                                '200':previous_df.iloc[-1]['200']
                            },
                            'last_update': df.iloc[-1]['date'],
                            'last_price': df.iloc[-1]['close'],
                    }
                elif pattern=="avg_QTotCap":
                    df['avg_QTotCap_1']=(df.iloc[-1]["QTotCap"]) / 1
                    df['avg_QTotCap_5']=(df.iloc[-5:]["QTotCap"].sum()) / 5
                    df['avg_QTotCap_10']=(df.iloc[-10:]["QTotCap"].sum()) / 10
                    df['avg_QTotCap_20']=(df.iloc[-20:]["QTotCap"].sum()) / 20
                    df['avg_QTotCap_50']=(df.iloc[-50:]["QTotCap"].sum()) / 50
                    df['avg_QTotCap_100']=(df.iloc[-100:]["QTotCap"].sum()) / 100
                    df['avg_QTotCap_200']=(df.iloc[-200:]["QTotCap"].sum()) / 200
                    
                    previous_df['avg_QTotCap_1']=(previous_df.iloc[-1]["QTotCap"]) / 1
                    previous_df['avg_QTotCap_5']=(previous_df.iloc[-5:]["QTotCap"].sum()) / 5
                    previous_df['avg_QTotCap_10']=(previous_df.iloc[-10:]["QTotCap"].sum()) / 10
                    previous_df['avg_QTotCap_20']=(previous_df.iloc[-20:]["QTotCap"].sum()) / 20
                    previous_df['avg_QTotCap_50']=(previous_df.iloc[-50:]["QTotCap"].sum()) / 50
                    previous_df['avg_QTotCap_100']=(previous_df.iloc[-100:]["QTotCap"].sum()) / 100
                    previous_df['avg_QTotCap_200']=(previous_df.iloc[-200:]["QTotCap"].sum()) / 200
                    
                    symbols_return[pattern]={
                        'value':{
                            '1':df.iloc[-1]['avg_QTotCap_1'],
                            '5':df.iloc[-1]['avg_QTotCap_5'],
                            '10':df.iloc[-1]['avg_QTotCap_10'],
                            '20':df.iloc[-1]['avg_QTotCap_20'],
                            '50':df.iloc[-1]['avg_QTotCap_50'],
                            '100':df.iloc[-1]['avg_QTotCap_100'],
                            '200':df.iloc[-1]['avg_QTotCap_200']
                        },
                        'previous':{
                            '1':previous_df.iloc[-1]['avg_QTotCap_1'],
                            '5':previous_df.iloc[-1]['avg_QTotCap_5'],
                            '10':previous_df.iloc[-1]['avg_QTotCap_10'],
                            '20':previous_df.iloc[-1]['avg_QTotCap_20'],
                            '50':previous_df.iloc[-1]['avg_QTotCap_50'],
                            '100':previous_df.iloc[-1]['avg_QTotCap_100'],
                            '200':previous_df.iloc[-1]['avg_QTotCap_200']
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                elif pattern=="avg_QTotTran5J":
                    df['avg_QTotTran5J_1']=(df.iloc[-1]["QTotTran5J"]) / 1
                    df['avg_QTotTran5J_5']=(df.iloc[-5:]["QTotTran5J"].sum()) / 5
                    df['avg_QTotTran5J_10']=(df.iloc[-10:]["QTotTran5J"].sum()) / 10
                    df['avg_QTotTran5J_20']=(df.iloc[-20:]["QTotTran5J"].sum()) / 20
                    df['avg_QTotTran5J_50']=(df.iloc[-50:]["QTotTran5J"].sum()) / 50
                    df['avg_QTotTran5J_100']=(df.iloc[-100:]["QTotTran5J"].sum()) / 100
                    df['avg_QTotTran5J_200']=(df.iloc[-200:]["QTotTran5J"].sum()) / 200
                    
                    previous_df['avg_QTotTran5J_1']=(previous_df.iloc[-1]["QTotTran5J"]) / 1
                    previous_df['avg_QTotTran5J_5']=(previous_df.iloc[-5:]["QTotTran5J"].sum()) / 5
                    previous_df['avg_QTotTran5J_10']=(previous_df.iloc[-10:]["QTotTran5J"].sum()) / 10
                    previous_df['avg_QTotTran5J_20']=(previous_df.iloc[-20:]["QTotTran5J"].sum()) / 20
                    previous_df['avg_QTotTran5J_50']=(previous_df.iloc[-50:]["QTotTran5J"].sum()) / 50
                    previous_df['avg_QTotTran5J_100']=(previous_df.iloc[-100:]["QTotTran5J"].sum()) / 100
                    previous_df['avg_QTotTran5J_200']=(previous_df.iloc[-200:]["QTotTran5J"].sum()) / 200
                    
                    symbols_return[pattern]={
                        'value':{
                            '1':df.iloc[-1]['avg_QTotTran5J_1'],
                            '5':df.iloc[-1]['avg_QTotTran5J_5'],
                            '10':df.iloc[-1]['avg_QTotTran5J_10'],
                            '20':df.iloc[-1]['avg_QTotTran5J_20'],
                            '50':df.iloc[-1]['avg_QTotTran5J_50'],
                            '100':df.iloc[-1]['avg_QTotTran5J_100'],
                            '200':df.iloc[-1]['avg_QTotTran5J_200']
                        },
                        'previous':{
                            '1':previous_df.iloc[-1]['avg_QTotTran5J_1'],
                            '5':previous_df.iloc[-1]['avg_QTotTran5J_5'],
                            '10':previous_df.iloc[-1]['avg_QTotTran5J_10'],
                            '20':previous_df.iloc[-1]['avg_QTotTran5J_20'],
                            '50':previous_df.iloc[-1]['avg_QTotTran5J_50'],
                            '100':previous_df.iloc[-1]['avg_QTotTran5J_100'],
                            '200':previous_df.iloc[-1]['avg_QTotTran5J_200']
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                elif pattern=="avg_ZTotTran": #average of total transactions per day
                    df['avg_ZTotTran_1']=(df.iloc[-1]["ZTotTran"]) / 1
                    df['avg_ZTotTran_5']=(df.iloc[-5:]["ZTotTran"].sum()) / 5
                    df['avg_ZTotTran_10']=(df.iloc[-10:]["ZTotTran"].sum()) / 10
                    df['avg_ZTotTran_20']=(df.iloc[-20:]["ZTotTran"].sum()) / 20
                    df['avg_ZTotTran_50']=(df.iloc[-50:]["ZTotTran"].sum()) / 50
                    df['avg_ZTotTran_100']=(df.iloc[-100:]["ZTotTran"].sum()) / 100
                    df['avg_ZTotTran_200']=(df.iloc[-200:]["ZTotTran"].sum()) / 200
                    
                    previous_df['avg_ZTotTran_1']=(previous_df.iloc[-1]["ZTotTran"]) / 1
                    previous_df['avg_ZTotTran_5']=(previous_df.iloc[-5:]["ZTotTran"].sum()) / 5
                    previous_df['avg_ZTotTran_10']=(previous_df.iloc[-10:]["ZTotTran"].sum()) / 10
                    previous_df['avg_ZTotTran_20']=(previous_df.iloc[-20:]["ZTotTran"].sum()) / 20
                    previous_df['avg_ZTotTran_50']=(previous_df.iloc[-50:]["ZTotTran"].sum()) / 50
                    previous_df['avg_ZTotTran_100']=(previous_df.iloc[-100:]["ZTotTran"].sum()) / 100
                    previous_df['avg_ZTotTran_200']=(previous_df.iloc[-200:]["ZTotTran"].sum()) / 200
                     
                    symbols_return[pattern]={
                        'value':{
                            '1':df.iloc[-1]['avg_ZTotTran_1'],
                            '5':df.iloc[-1]['avg_ZTotTran_5'],
                            '10':df.iloc[-1]['avg_ZTotTran_10'],
                            '20':df.iloc[-1]['avg_ZTotTran_20'],
                            '50':df.iloc[-1]['avg_ZTotTran_50'],
                            '100':df.iloc[-1]['avg_ZTotTran_100'],
                            '200':df.iloc[-1]['avg_ZTotTran_200']
                        },
                        'previous':{
                            '1':previous_df.iloc[-1]['avg_ZTotTran_1'],
                            '5':previous_df.iloc[-1]['avg_ZTotTran_5'],
                            '10':previous_df.iloc[-1]['avg_ZTotTran_10'],
                            '20':previous_df.iloc[-1]['avg_ZTotTran_20'],
                            '50':previous_df.iloc[-1]['avg_ZTotTran_50'],
                            '100':previous_df.iloc[-1]['avg_ZTotTran_100'],
                            '200':previous_df.iloc[-1]['avg_ZTotTran_200']
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                       
                    
                    


            if symbols_return:

                self.update_key(symbol,symbols_return)
                print("%s updated" %symbol)
            else:
                print('symbols is none')


        print("%s --- %s seconds ---" % (time.strftime("%a, %d %b %Y %H:%M:%S", time.gmtime()),(time.time() - start_time)))

    def get_data(self,Inscode):
        main_dict = {}
        now = datetime.now()
        time_tset_now = now.strftime("%Y%m%d")        
        username = "idenegar.com"
        password = "!D3n3g@r.C0m"
        url = "http://service.tsetmc.com/WebService/TsePublicV2.asmx"
        headers = {
        'Content-Type': 'application/soap+xml; charset=utf-8',
        'Host': 'service.tsetmc.com'
        }
        
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
        <soap12:Body>
        <InstTrade xmlns="http://tsetmc.com/">
            <UserName>{username}</UserName>
            <Password>{password}</Password>
            <Inscode>{Inscode}</Inscode>
            <DateFrom>20200601</DateFrom>
            <DateTo>{time_tset_now}</DateTo>
            </InstTrade>
        </soap12:Body>
        </soap12:Envelope>"""     
        while True:
            try:
                response = requests.post(url, headers=headers, data=payload)
                all = xmltodict.parse(response.text)
                diffgr = all['soap:Envelope']['soap:Body']['InstTradeResponse']['InstTradeResult']['diffgr:diffgram']['TradeSelectedDate']['TradeSelectedDate']
                break
            except:
                print('ib connection error')
                #print(response.text)
                time.sleep(1)
                continue
        main_dict[Inscode]=[]
        for i in diffgr:
            if float(i['PriceMin'])<=0:
                continue
            InsCode = i['InsCode']
            re_json = {
                "date": i['DEven'],
                "HEven": i['HEven'],
                "close": float(i['PClosing']),
                "IClose": float(i['IClose']),
                "YClose": float(i['YClose']),
                "PDrCotVal": float(i['PDrCotVal']),
                "QTotTran5J": float(i['QTotTran5J']),
                "ZTotTran": float(i['ZTotTran']),
                "volume": int(i['QTotTran5J']),
                "QTotCap": float(i['QTotCap']),
                "PriceChange": float(i['PriceChange']),
                "low": float(i['PriceMin']),
                "high": float(i['PriceMax']),
                "PriceYesterday": float(i['PriceYesterday']),
                "open": float(i['PriceFirst']),
                
            }
            if InsCode not in main_dict.keys():
                main_dict[Inscode] = []
            main_dict[InsCode].append(re_json)    
            
        return main_dict
                           
    def getSymbols(self):
        return self.getExchangeSymbols("tsetmc")
        
        return {"5054819322815158"}
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
        
        patterns = ["rate_of_return","avg_QTotCap","avg_QTotTran5J", "avg_ZTotTran"]

        allSymbols=self.getSymbols()
        
        for symbols in zip(*(iter(allSymbols),) * 20):
            self.calculate_indicator(symbols,patterns,"1d")
            print('sleep')
            time.sleep(60)
            
        
    def getExchangeSymbols(self,exchange):
        url = 'https://feed.tseshow.com/api/stockInSector'
        try:
            x = requests.get(url, verify=False)
 
            x = json.loads(x.text)
            x= x['data']
        except:
            return []
        symbols=[]
        for (key,val) in x.items():
            if val["YVal"]=="300":
                symbols.append(key)
                        
        return symbols
    
    def update_key(self,symbol,symbols_return):
        # if symbol in self.crypto_symbol:
        #     sql = "UPDATE `crypto_indicators` SET `rsi`=%s,`macd`=%s,`uo`=%s,`roc`=%s,`ema-10`=%s,`ema-20`=%s,`ema-50`=%s,`ema-100`=%s,`ema-200`=%s,`sma-10`=%s,`sma-20`=%s,`sma-50`=%s,`sma-100`=%s,`sma-200`=%s,`stoch`=%s,`adx`=%s,`cci-20`=%s,`chaikin-money-flow`=%s,`stoch-rsi`=%s,`williams`=%s,`atr-14`=%s,`money-flow-index`=%s WHERE `crypto_id`= (SELECT `id` From `cryptos` WHERE `symbol`=%s LIMIT 1)"
        # elif symbol in self.forex_symbol:
        #     sql = "UPDATE `currency_indicators` SET `rsi`=%s,`macd`=%s,`uo`=%s,`roc`=%s,`ema-10`=%s,`ema-20`=%s,`ema-50`=%s,`ema-100`=%s,`ema-200`=%s,`sma-10`=%s,`sma-20`=%s,`sma-50`=%s,`sma-100`=%s,`sma-200`=%s,`stoch`=%s,`adx`=%s,`cci-20`=%s,`chaikin-money-flow`=%s,`stoch-rsi`=%s,`williams`=%s,`atr-14`=%s,`money-flow-index`=%s WHERE `pair_id`= (SELECT `id` From `currencies` WHERE `pair`=%s LIMIT 1)"
        # else:
        
        
        sql = "UPDATE `stock_rates` SET `rate_1`=%s,`rate_5`=%s, `rate_10`=%s, `rate_20`=%s, `rate_50`=%s , `rate_100`=%s, `rate_200`=%s,`avg_QTotCap_1`=%s, `avg_QTotCap_5`=%s , `avg_QTotCap_10`=%s, `avg_QTotCap_20`=%s,  `avg_QTotCap_50`=%s, `avg_QTotCap_100`=%s , `avg_QTotCap_200`=%s ,`avg_QTotTran5J_1`=%s, `avg_QTotTran5J_5`=%s , `avg_QTotTran5J_10`=%s, `avg_QTotTran5J_20`=%s,  `avg_QTotTran5J_50`=%s, `avg_QTotTran5J_100`=%s , `avg_QTotTran5J_200`=%s ,`avg_ZTotTran_1`=%s, `avg_ZTotTran_5`=%s , `avg_ZTotTran_10`=%s, `avg_ZTotTran_20`=%s,  `avg_ZTotTran_50`=%s, `avg_ZTotTran_100`=%s , `avg_ZTotTran_200`=%s)"
        sql = sql + "WHERE `Inscode`=%s"
        mydb = mysql.connector.connect(user = self.mysqluser, host = self.mySqlHost, database = self.mySqlDBName)
        val = (
            float(symbols_return['rate_of_return']['value']['1']) if not math.isnan(symbols_return['rate_of_return']['value']['1']) and not math.isinf(symbols_return['rate_of_return']['value']['1']) else 0,
            float(symbols_return['rate_of_return']['value']['5']) if not math.isnan(symbols_return['rate_of_return']['value']['5']) and not math.isinf(symbols_return['rate_of_return']['value']['5']) else 0,
            float(symbols_return['rate_of_return']['value']['10']) if not math.isnan(symbols_return['rate_of_return']['value']['10']) and not math.isinf(symbols_return['rate_of_return']['value']['10']) else 0,
            float(symbols_return['rate_of_return']['value']['20']) if not math.isnan(symbols_return['rate_of_return']['value']['20']) and not math.isinf(symbols_return['rate_of_return']['value']['20']) else 0,
            float(symbols_return['rate_of_return']['value']['50']) if not math.isnan(symbols_return['rate_of_return']['value']['50']) and not math.isinf(symbols_return['rate_of_return']['value']['50']) else 0,
            float(symbols_return['rate_of_return']['value']['100']) if not math.isnan(symbols_return['rate_of_return']['value']['100']) and not math.isinf(symbols_return['rate_of_return']['value']['100']) else 0,
            float(symbols_return['rate_of_return']['value']['200']) if not math.isnan(symbols_return['rate_of_return']['value']['200']) and not math.isinf(symbols_return['rate_of_return']['value']['200']) else 0,
            float(symbols_return['avg_QTotCap']['value']['1']) if not math.isnan(symbols_return['avg_QTotCap']['value']['1']) and not math.isinf(symbols_return['avg_QTotCap']['value']['1']) else 0,
            float(symbols_return['avg_QTotCap']['value']['5']) if not math.isnan(symbols_return['avg_QTotCap']['value']['5']) and not math.isinf(symbols_return['avg_QTotCap']['value']['5']) else 0,
            float(symbols_return['avg_QTotCap']['value']['10']) if not math.isnan(symbols_return['avg_QTotCap']['value']['10']) and not math.isinf(symbols_return['avg_QTotCap']['value']['10']) else 0,
            float(symbols_return['avg_QTotCap']['value']['20']) if not math.isnan(symbols_return['avg_QTotCap']['value']['20']) and not math.isinf(symbols_return['avg_QTotCap']['value']['20']) else 0,
            float(symbols_return['avg_QTotCap']['value']['50']) if not math.isnan(symbols_return['avg_QTotCap']['value']['50']) and not math.isinf(symbols_return['avg_QTotCap']['value']['50']) else 0,
            float(symbols_return['avg_QTotCap']['value']['100']) if not math.isnan(symbols_return['avg_QTotCap']['value']['100']) and not math.isinf(symbols_return['avg_QTotCap']['value']['100']) else 0,
            float(symbols_return['avg_QTotCap']['value']['200']) if not math.isnan(symbols_return['avg_QTotCap']['value']['200']) and not math.isinf(symbols_return['avg_QTotCap']['value']['200']) else 0,
            float(symbols_return['avg_QTotTran5J']['value']['1']) if not math.isnan(symbols_return['avg_QTotTran5J']['value']['1']) and not math.isinf(symbols_return['avg_QTotTran5J']['value']['1']) else 0,
            float(symbols_return['avg_QTotTran5J']['value']['5']) if not math.isnan(symbols_return['avg_QTotTran5J']['value']['5']) and not math.isinf(symbols_return['avg_QTotTran5J']['value']['5']) else 0,
            float(symbols_return['avg_QTotTran5J']['value']['10']) if not math.isnan(symbols_return['avg_QTotTran5J']['value']['10']) and not math.isinf(symbols_return['avg_QTotTran5J']['value']['10']) else 0,
            float(symbols_return['avg_QTotTran5J']['value']['20']) if not math.isnan(symbols_return['avg_QTotTran5J']['value']['20']) and not math.isinf(symbols_return['avg_QTotTran5J']['value']['20']) else 0,
            float(symbols_return['avg_QTotTran5J']['value']['50']) if not math.isnan(symbols_return['avg_QTotTran5J']['value']['50']) and not math.isinf(symbols_return['avg_QTotTran5J']['value']['50']) else 0,
            float(symbols_return['avg_QTotTran5J']['value']['100']) if not math.isnan(symbols_return['avg_QTotTran5J']['value']['100']) and not math.isinf(symbols_return['avg_QTotTran5J']['value']['100']) else 0,
            float(symbols_return['avg_QTotTran5J']['value']['200']) if not math.isnan(symbols_return['avg_QTotTran5J']['value']['200']) and not math.isinf(symbols_return['avg_QTotTran5J']['value']['200']) else 0,
            float(symbols_return['avg_ZTotTran']['value']['1']) if not math.isnan(symbols_return['avg_ZTotTran']['value']['1']) and not math.isinf(symbols_return['avg_ZTotTran']['value']['1']) else 0,
            float(symbols_return['avg_ZTotTran']['value']['5']) if not math.isnan(symbols_return['avg_ZTotTran']['value']['5']) and not math.isinf(symbols_return['avg_ZTotTran']['value']['5']) else 0,
            float(symbols_return['avg_ZTotTran']['value']['10']) if not math.isnan(symbols_return['avg_ZTotTran']['value']['10']) and not math.isinf(symbols_return['avg_ZTotTran']['value']['10']) else 0,
            float(symbols_return['avg_ZTotTran']['value']['20']) if not math.isnan(symbols_return['avg_ZTotTran']['value']['20']) and not math.isinf(symbols_return['avg_ZTotTran']['value']['20']) else 0,
            float(symbols_return['avg_ZTotTran']['value']['50']) if not math.isnan(symbols_return['avg_ZTotTran']['value']['50']) and not math.isinf(symbols_return['avg_ZTotTran']['value']['50']) else 0,
            float(symbols_return['avg_ZTotTran']['value']['100']) if not math.isnan(symbols_return['avg_ZTotTran']['value']['100']) and not math.isinf(symbols_return['avg_ZTotTran']['value']['100']) else 0,
            float(symbols_return['avg_ZTotTran']['value']['200']) if not math.isnan(symbols_return['avg_ZTotTran']['value']['200']) and not math.isinf(symbols_return['avg_ZTotTran']['value']['200']) else 0,
            
            
           
            


            

            symbol
        )

        mycursor = mydb.cursor()
        #print(sql%(val))
        r=mycursor.execute(sql, val)
        mydb.commit()
        try:
            if(mycursor.rowcount==0):
                sql ="INSERT INTO `stock_rates` (`rate_1`, `rate_5`, `rate_10`, `rate_20`, `rate_50`, `rate_100`, `rate_200`,`avg_QTotCap_1`, `avg_QTotCap_5` , `avg_QTotCap_10`, `avg_QTotCap_20`,  `avg_QTotCap_50`, `avg_QTotCap_100` , `avg_QTotCap_200`,`avg_QTotTran5J_1`, `avg_QTotTran5J_5` , `avg_QTotTran5J_10`, `avg_QTotTran5J_20`,  `avg_QTotTran5J_50`, `avg_QTotTran5J_100` , `avg_QTotTran5J_200` ,`avg_ZTotTran_1`, `avg_ZTotTran_5` , `avg_ZTotTran_10`, `avg_ZTotTran_20`,  `avg_ZTotTran_50`, `avg_ZTotTran_100` , `avg_ZTotTran_200` ,`Inscode`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                r=mycursor.execute(sql, val)
                mydb.commit()
                print("Inserted")
            print(mycursor.rowcount, "details inserted")
        except:
            pass
        
        mydb.close()


print('run service')


q=queue.Queue()

def now_time_run():
    now = datetime.now()
    time_tset_now = now.strftime("%H%M")
    weekday=datetime.today().weekday()
    print(time_tset_now)
    s_t = "1600"
    e_t = "2301"
    if time_tset_now <e_t and time_tset_now >= s_t and weekday in [6,5,0,1,2]:
        exit_run = False
    else:
        exit_run = True
    return exit_run


if __name__ == '__main__':
    try:
        indicator=IndicatorUpdate()
        while True:
            while now_time_run():
                print("Exit time")
                #break
                time.sleep(60)
        
            indicator.load_machines()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
