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
from datetime import datetime, timedelta
import psycopg2
from os import environ
import urllib3

DB_USER = environ.get("FOM_DB_USER", default='db_tseshow_user')
DB_PASS = environ.get("FOM_DB_PASSWORD", default='l8PDQGtKyMvynFb')
DB_HOST = environ.get("FOM_DB_HOST", default='87.107.188.201')
DB_PORT = environ.get("FOM_DB_PORT", default='6033')
DB_NAME = environ.get("FOM_DB_NAME", default='stockfeeder')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class IndicatorUpdate(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.user= DB_USER
        self.password= DB_PASS
        self.host= DB_HOST
        self.dbName= DB_NAME 
        self.dbPort= DB_PORT
        self.getNoavaranSymbol={}
        self.token=""
        self.tokenEx=""
        NoavaranToken = open("NoavaranToken.json")
        data = json.load(NoavaranToken)
        self.token=data['token']['token']
        self.tokenEx=datetime.strptime(data['ex'], '%Y-%m-%d %H:%M:%S.%f')            
        self.getNoavaranSymbol=self.getNoavaranExchangeSymbols()



    def get_database_number(self,interval):
            return 10

    def get_token(self)->str:
        url = "https://data3.nadpco.com/api/v2/Token"
        
        username = "FFV147110053"
        password = "DDYEcPqgHdgUeAS"
        
        payload={}
        now = datetime.now()
        #datetime.strptime(request['noavaran']['TokenEx'], '%y/%m/%d %H:%M:%S')
        
        if not self.tokenEx or now>self.tokenEx:
            response = requests.request("POST", url, auth=(username, password), data=payload)
            data = response.text
            #data = r"{'token': '0D82D591DF32F6F4FC8B557BB6892A14F0D77E7168F95C86CF5154D75BB3D825F939E4E92C430824FFAB89BC1734786305CD6667A53BD8AD31F98558BC05BE0A'}"
            data= data.replace("\'", "\"")

            
            data =json.loads(data)
            
            print(data['token'])
            
            
            now = now + timedelta(hours=6)
            json_object = json.dumps({"token":data,"ex":now}, indent=4, sort_keys=True, default=str)
            with open("NoavaranToken.json", "w") as outfile:
                outfile.write(json_object)
                
            self.tokenEx = now
            self.token= data['token']
            return self.token
            
        else:
            return self.token                

          
    def connect_to_mysql(self):
        # Connecting from the server
        while True:
            try:
                conn = mysql.connector.connect(user = self.user,password=self.password, host = self.host,port=self.dbPort,auth_plugin='mysql_native_password',  database = self.dbName)
                return conn
            except:
                time.sleep(60)
                continue
            
    def connect_to_pg(self):
        connection = psycopg2.connect(user=self.user,
                                password=self.password,
                                host=self.host,
                                port=self.dbPort,
                                database=self.dbName)
        return connection
   
    def calculate_indicator(self,symbols,patterns,interval):
        start_time = time.time()



        symbols_return={}

        for symbol in symbols:

            r = self.get_data_noavaran(symbol)
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

                if pattern=="macd":
                    df['MACD_Histogram']=ta.trend.macd_diff(close=df["close"])
                    df['MACD_Line']=ta.trend.macd(close=df["close"])
                    df['Signal_Line']=ta.trend.macd_signal(close=df["close"])


                    previous_df['MACD_Histogram']=ta.trend.macd_diff(close=previous_df["close"])
                    previous_df['MACD_Line']=ta.trend.macd(close=previous_df["close"])
                    previous_df['Signal_Line']=ta.trend.macd_signal(close=previous_df["close"])

                    symbols_return[pattern]={
                            'value':{
                                'MACD-Histogram':df.iloc[-1]['MACD_Histogram'],
                                'MACD-Line':df.iloc[-1]['MACD_Line'],
                                'Signal-Line':df.iloc[-1]['Signal_Line'],
                            },
                            'previous':{
                                'MACD-Histogram':previous_df.iloc[-1]['MACD_Histogram'],
                                'MACD-Line':previous_df.iloc[-1]['MACD_Line'],
                                'Signal-Line':previous_df.iloc[-1]['Signal_Line'],
                            },
                            'last_update': df.iloc[-1]['date'],
                            'last_price': df.iloc[-1]['close'],
                    }

                elif pattern=="rsi":
                    df['RSI']=ta.momentum.rsi(close=df["close"])
                    previous_df['RSI']= ta.momentum.rsi(close=previous_df["close"])
                    symbols_return[pattern]={
                        'value':{
                            'RSI':df.iloc[-1]['RSI'],
                        },
                        'previous':{
                            'RSI':previous_df.iloc[-1]['RSI'],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }

                elif pattern=="money-flow-index":
                    df['MFI']=ta.volume.money_flow_index(high= df["high"], low= df["low"], close= df["close"], volume= df["volume"])
                    previous_df['MFI']=ta.volume.money_flow_index(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"], volume= previous_df["volume"])
                    symbols_return[pattern]={
                        'value':{
                            'MFI':df.iloc[-1]['MFI'],
                        },
                        'previous':{
                            'MFI':previous_df.iloc[-1]['MFI'],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }

                elif pattern=="vwap":
                    #df[pattern]=ta.volume.volume_weighted_average_price(high= df["high"], low= df["low"], close= df["close"], volume= df["volume"])
                    #previous_df[pattern]=ta.volume.volume_weighted_average_price(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"], volume= previous_df["volume"])
                    df[pattern]=ScanicalIndicator().VWAP(series_buffer[:-1])
                    previous_df[pattern]=ScanicalIndicator().VWAP(series_buffer[:-2])

                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                    symbols_return[pattern]['Close-Price']=df.iloc[-1]['close']
                    symbols_return[pattern]['Low-Price']=df.iloc[-1]['low']
                    symbols_return[pattern]['High-Price']=df.iloc[-1]['high']

                    symbols_return[pattern]['previous']['Close-Price']=previous_df.iloc[-1]['close']
                    symbols_return[pattern]['previous']['Low-Price']=previous_df.iloc[-1]['low']
                    symbols_return[pattern]['previous']['High-Price']=previous_df.iloc[-1]['high']

                elif pattern=="atr-14":
                    if len(series_buffer)<=50:
                        print ("Symbol %s is count<50 in timeframe %s" %(symbol,interval))
                        continue
                    df[pattern]=ta.volatility.average_true_range(high= df["high"], low= df["low"], close= df["close"])
                    previous_df[pattern]=ta.volatility.average_true_range(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"])
                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }

                elif pattern=="sma-10":
                    SMAPeriod=[10]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in SMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.sma_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.sma_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])

                    symbols_return.update({pattern:multivalue})
                    
                    
                    
                elif pattern=="sma-20":
                    
                    SMAPeriod=[20]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in SMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.sma_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.sma_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])


                    symbols_return.update({pattern:multivalue})
                    
                    
                elif pattern=="sma-50":
                    SMAPeriod=[50]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in SMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.sma_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.sma_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])


                    symbols_return.update({pattern:multivalue})
                elif pattern=="sma-100":
                    SMAPeriod=[100]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in SMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.sma_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.sma_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])


                    symbols_return.update({pattern:multivalue})
                elif pattern=="sma-200":
                    SMAPeriod=[200]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in SMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.sma_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.sma_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])


                    symbols_return.update({pattern:multivalue})

                elif pattern=="ema-10":
                    EMAPeriod=[10]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in EMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.ema_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.ema_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])


                    symbols_return.update({pattern:multivalue})
                elif pattern=="ema-20":
                    EMAPeriod=[20]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in EMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.ema_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.ema_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])


                    symbols_return.update({pattern:multivalue})
                elif pattern=="ema-50":
                    EMAPeriod=[50]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in EMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.ema_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.ema_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])


                    symbols_return.update({pattern:multivalue})
                elif pattern=="ema-100":
                    EMAPeriod=[100]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in EMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.ema_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.ema_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])


                    symbols_return.update({pattern:multivalue})
                elif pattern=="ema-200":
                    EMAPeriod=[200]
                    multivalue={
                        'value':{
                        },
                        'previous':{
                        },
                    }
                    for n_period in EMAPeriod:
                        df[str(n_period)+'Length']=ta.trend.ema_indicator(close= df["close"], window=n_period)
                        previous_df[str(n_period)+'Length']=ta.trend.ema_indicator(close= previous_df["close"], window=n_period)
                        multivalue['value']=df.iloc[-1][str(n_period)+'Length']
                        multivalue['previous']=previous_df.iloc[-1][str(n_period)+'Length']

                    multivalue['last_update']=df.iloc[-1]['date']
                    multivalue['last_price']=float(df.iloc[-1]['close'])

                    multivalue['Close-Price']=float(df.iloc[-1]['close'])
                    multivalue['Low-Price']=float(df.iloc[-1]['low'])
                    multivalue['High-Price']=float(df.iloc[-1]['high'])


                    symbols_return.update({pattern:multivalue})

                elif pattern=="cci-20":
                    df[pattern]=ta.trend.cci(high= df["high"], low= df["low"], close= df["close"])
                    previous_df[pattern]=ta.trend.cci(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"])
                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }

                elif pattern=="stoch":
                    df[pattern]=ta.momentum.stoch(high= df["high"], low= df["low"], close= df["close"])
                    previous_df[pattern]=ta.momentum.stoch(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"])

                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }

                    #print(symbols)

                elif pattern=="adx":
                    # try:
                    df[pattern]=ta.trend.adx(high= df["high"], low= df["low"], close= df["close"])
                    df["adx_positive"]=ta.trend.adx_pos(high= df["high"], low= df["low"], close= df["close"])
                    df["adx_negative"]=ta.trend.adx_neg(high= df["high"], low= df["low"], close= df["close"])
                    
                    previous_df[pattern]=ta.trend.adx(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"])
                    previous_df["adx_positive"]=ta.trend.adx_pos(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"])
                    previous_df["adx_negative"]=ta.trend.adx_neg(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"])
                    
                    # except:
                    #     pass
                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                            'adx_positive':df.iloc[-1]['adx_positive'],
                            'adx_negative':df.iloc[-1]['adx_negative'],
                            
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                            'adx_positive':previous_df.iloc[-1]['adx_positive'],
                            'adx_negative':previous_df.iloc[-1]['adx_negative'],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                    
                elif pattern=="uo":
                    
                    df[pattern]=ta.momentum.ultimate_oscillator(high= df["high"], low= df["low"], close= df["close"])
                    previous_df[pattern]=ta.momentum.ultimate_oscillator(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"])
                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                elif pattern=="roc":
                    df[pattern]=ta.momentum.roc(close= df["close"])
                    previous_df[pattern]=ta.momentum.roc(close= previous_df["close"])
                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                elif pattern=="chaikin-money-flow":
                    df[pattern]=ta.volume.chaikin_money_flow(high= df["high"], low= df["low"], close= df["close"], volume= df["volume"])
                    previous_df[pattern]=ta.volume.chaikin_money_flow(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"], volume= previous_df["volume"])
                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                elif pattern=="williams":
                    
                    df[pattern]=ta.momentum.williams_r(high= df["high"], low= df["low"], close= df["close"])
                    previous_df[pattern]=ta.momentum.williams_r(high= previous_df["high"], low= previous_df["low"], close= previous_df["close"])
                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                elif pattern=="stoch-rsi":
                    df[pattern]=ta.momentum.stochrsi(close= df["close"])
                    previous_df[pattern]=ta.momentum.stochrsi(close= previous_df["close"])

                    symbols_return[pattern]={
                        'value':{
                            pattern:df.iloc[-1][pattern],
                        },
                        'previous':{
                            pattern:previous_df.iloc[-1][pattern],
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                elif pattern=="historical_price":
                    #get index max value of df in column high
                    df["hist_high"]=df.iloc[df["high"].idxmax()].high
                    df["hist_low"]=df.iloc[df["low"].idxmin()].low
                    df["hist_date_low"]=df.iloc[df["low"].idxmin()].date
                    df["hist_date_high"]=df.iloc[df["high"].idxmin()].date
                    #get index max value of previous_df in column high
                    previous_df["hist_high"]=previous_df.iloc[previous_df["high"].idxmax()].high
                    previous_df["hist_low"]=previous_df.iloc[previous_df["low"].idxmin()].low
                    previous_df["hist_date_low"]=previous_df.iloc[previous_df["low"].idxmin()].date
                    previous_df["hist_date_high"]=previous_df.iloc[previous_df["high"].idxmin()].date
                    
                    

                    symbols_return[pattern]={
                        'value':{
                            "high":df.iloc[-1]["hist_high"],
                            "low":df.iloc[-1]["hist_low"],
                            "date_low":df.iloc[-1]["hist_date_low"],
                            "date_high":df.iloc[-1]["hist_date_high"],
                        },
                        'previous':{
                            "high":previous_df.iloc[-1]["hist_high"],
                            "low":previous_df.iloc[-1]["hist_low"],
                            "date_low":previous_df.iloc[-1]["hist_date_low"],
                            "date_high":previous_df.iloc[-1]["hist_date_high"],
                            
                        },
                        'last_update': df.iloc[-1]['date'],
                        'last_price': df.iloc[-1]['close'],
                    }
                        #print(symbols)
                elif pattern=="ichimoku":
                    df['ichimoku_a']=ta.trend.ichimoku_a(high=df["high"],low=df['low'])
                    df['ichimoku_b']=ta.trend.ichimoku_b(high=df["high"],low=df['low'])
                    df['ichimoku_base_line']=ta.trend.ichimoku_base_line(high=df["high"],low=df['low'])
                    df['ichimoku_conversion_line'] = ta.trend.ichimoku_conversion_line(high=df["high"],low=df['low'])
                    

                    previous_df['ichimoku_a']=ta.trend.ichimoku_a(high=previous_df["high"],low=previous_df['low'])
                    previous_df['ichimoku_b']=ta.trend.ichimoku_b(high=previous_df["high"],low=previous_df['low'])
                    previous_df['ichimoku_base_line']=ta.trend.ichimoku_base_line(high=previous_df["high"],low=previous_df['low'])
                    previous_df['ichimoku_conversion_line'] = ta.trend.ichimoku_conversion_line(high=previous_df["high"],low=previous_df['low'])
                    

                    symbols_return[pattern]={
                            'value':{
                                'ichimoku_a':df.iloc[-1]['ichimoku_a'],
                                'ichimoku_b':df.iloc[-1]['ichimoku_b'],
                                'ichimoku_base_line':df.iloc[-1]['ichimoku_base_line'],
                                'ichimoku_conversion_line':df.iloc[-1]['ichimoku_conversion_line'],
                            },
                            'previous':{
                                'ichimoku_a':previous_df.iloc[-1]['ichimoku_a'],
                                'ichimoku_b':previous_df.iloc[-1]['ichimoku_b'],
                                'ichimoku_base_line':previous_df.iloc[-1]['ichimoku_base_line'],
                                'ichimoku_conversion_line':previous_df.iloc[-1]['ichimoku_conversion_line'],
                            },
                            'last_update': df.iloc[-1]['date'],
                            'last_price': df.iloc[-1]['close'],
                    }

                elif pattern=="StochasticOscillator":
                    df['StochasticOscillator']=ta.momentum.StochasticOscillator(high=df["high"], low=df["low"], close= df["close"]).stoch()
                    df['stoch_signal']=ta.momentum.StochasticOscillator(high=df["high"], low=df["low"], close= df["close"]).stoch_signal()



                    previous_df['StochasticOscillator']= ta.momentum.StochasticOscillator(high=previous_df["high"], low=previous_df["low"], close= previous_df["close"]).stoch()
                    previous_df['stoch_signal']= ta.momentum.StochasticOscillator(high=previous_df["high"], low=previous_df["low"], close= previous_df["close"]).stoch_signal()
                     

                    symbols_return[pattern]={
                            'value':{
                                'StochasticOscillator':df.iloc[-1]['StochasticOscillator'],
                                'stoch_signal':df.iloc[-1]['stoch_signal'],
                            },
                            'previous':{
                                'StochasticOscillator':previous_df.iloc[-1]['StochasticOscillator'],
                                'stoch_signal':previous_df.iloc[-1]['stoch_signal'],
                            },
                            'last_update': df.iloc[-1]['date'],
                            'last_price': df.iloc[-1]['close'],
                    }

                elif pattern=="PSAR":
                    df['psar']=ta.trend.PSARIndicator(high=df["high"], low=df["low"], close= df["close"]).psar()
                    df['psar_down']=ta.trend.PSARIndicator(high=df["high"], low=df["low"], close= df["close"]).psar_down()
                    df['psar_down_indicator']=ta.trend.PSARIndicator(high=df["high"], low=df["low"], close= df["close"]).psar_down_indicator()
                    df['psar_up']=ta.trend.PSARIndicator(high=df["high"], low=df["low"], close= df["close"]).psar_up()
                    df['psar_up_indicator']=ta.trend.PSARIndicator(high=df["high"], low=df["low"], close= df["close"]).psar_up_indicator()
                    

                    previous_df['psar']= ta.trend.PSARIndicator(high=previous_df["high"], low=previous_df["low"], close= previous_df["close"]).psar()
                    previous_df['psar_down']= ta.trend.PSARIndicator(high=previous_df["high"], low=previous_df["low"], close= previous_df["close"]).psar_down()
                    previous_df['psar_down_indicator']= ta.trend.PSARIndicator(high=previous_df["high"], low=previous_df["low"], close= previous_df["close"]).psar_down_indicator()
                    previous_df['psar_up']= ta.trend.PSARIndicator(high=previous_df["high"], low=previous_df["low"], close= previous_df["close"]).psar_up()
                    previous_df['psar_up_indicator']= ta.trend.PSARIndicator(high=previous_df["high"], low=previous_df["low"], close= previous_df["close"]).psar_up_indicator()

                    symbols_return[pattern]={
                            'value':{
                                'psar':df.iloc[-1]['psar'],
                                'psar_down':df.iloc[-1]['psar_down'],
                                'psar_down_indicator':df.iloc[-1]['psar_down_indicator'],
                                'psar_up':df.iloc[-1]['psar_up'],
                                'psar_up_indicator':df.iloc[-1]['psar_up_indicator'],
                            },
                            'previous':{
                                'psar':previous_df.iloc[-1]['psar'],
                                'psar_down':previous_df.iloc[-1]['psar_down'],
                                'psar_down_indicator':previous_df.iloc[-1]['psar_down_indicator'],
                                'psar_up':previous_df.iloc[-1]['psar_up'],
                                'psar_up_indicator':previous_df.iloc[-1]['psar_up_indicator'],
                                
                            },
                            'last_update': df.iloc[-1]['date'],
                            'last_price': df.iloc[-1]['close'],
                    }


                # except NameError:
                #     print('An exception flew by!')
                #     raise
                # except:
                #     print("Unexpected error:", sys.exc_info()[0])
                #     continue







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

    def get_data_noavaran(self,Inscode):
        main_dict = {}
        now = datetime.now()
        time_tset_now = now.strftime("%Y%m%d")
        if Inscode in self.getNoavaranSymbol:
            pass
        else:
            main_dict[Inscode]=[]
            return main_dict
        
        url = "https://data3.nadpco.com/api/v3/TS/AdjustedTradesById/"+str(int(self.getNoavaranSymbol[Inscode]["coID"]))+"?fromdate=14000501"
        x=[] #price series  
        payload={}


        headers = {
            'Authorization': 'Bearer '+ self.get_token(),
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
            if response.status_code==200:
                data= response.text
                data=json.loads(data)
            else:
                time.sleep(60)
                return main_dict
        except:
            main_dict[Inscode]=[]
            print("error in get noavaran request go to next symbol")
            return main_dict
            
        for bar in data: 
        
            re_json={
                "date": datetime.strptime(bar["tradeDateGre"], "%Y-%m-%dT%H:%M:%S"),
                "open": float(bar["openingAdjPrice"]),
                "high": float(bar["maxAdjPrice"]),
                "low" : float(bar["minAdjPrice"]),
                "close" : float(bar["lastAdjPrice"]),
                "volume" : float(bar["tradeValue"]), 
            } 
            if Inscode not in main_dict.keys():
                main_dict[Inscode] = []
            main_dict[Inscode].append(re_json)    
            
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
        
        patterns = ["PSAR","stoch_signal","StochasticOscillator","ichimoku","historical_price","rsi","macd","uo","roc","ema-10","ema-20","ema-50","ema-100","ema-200","sma-10","sma-20","sma-50","sma-100","sma-200","stoch","adx","cci-20","chaikin-money-flow","stoch-rsi","williams","atr-14","money-flow-index"]

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
            if val["YVal"]=="300" or  val["YVal"]=="303" or  val["YVal"]=="305" or  val["YVal"]=="307" or  val["YVal"]=="309" or  val["YVal"]=="313" or  val["YVal"]=="300" or  val["YVal"]=="322" or  val["YVal"]=="323":
                symbols.append(key)
                        
        return symbols
    def getNoavaranExchangeSymbols(self):
        url = "https://data3.nadpco.com/api/v3/BaseInfo/Companies"

        payload={}
        headers = {}
        while True:
            try:
                response = requests.request("GET", url, headers=headers, data=payload)
                data= response.text
                Instrument=json.loads(data)
                break
            except:
                print('noavaran symbols connection error')
                #print(response.text)
                time.sleep(10)
                continue

        main_dict={}

        list=[]
        for c in Instrument:
            main_dict[c["tseCode"]]=c 
    
        return main_dict
    
   
    def update_key(self,symbol,symbols_return):
        # if symbol in self.crypto_symbol:
        #     sql = "UPDATE `crypto_indicators` SET `rsi`=%s,`macd`=%s,`uo`=%s,`roc`=%s,`ema-10`=%s,`ema-20`=%s,`ema-50`=%s,`ema-100`=%s,`ema-200`=%s,`sma-10`=%s,`sma-20`=%s,`sma-50`=%s,`sma-100`=%s,`sma-200`=%s,`stoch`=%s,`adx`=%s,`cci-20`=%s,`chaikin-money-flow`=%s,`stoch-rsi`=%s,`williams`=%s,`atr-14`=%s,`money-flow-index`=%s WHERE `crypto_id`= (SELECT `id` From `cryptos` WHERE `symbol`=%s LIMIT 1)"
        # elif symbol in self.forex_symbol:
        #     sql = "UPDATE `currency_indicators` SET `rsi`=%s,`macd`=%s,`uo`=%s,`roc`=%s,`ema-10`=%s,`ema-20`=%s,`ema-50`=%s,`ema-100`=%s,`ema-200`=%s,`sma-10`=%s,`sma-20`=%s,`sma-50`=%s,`sma-100`=%s,`sma-200`=%s,`stoch`=%s,`adx`=%s,`cci-20`=%s,`chaikin-money-flow`=%s,`stoch-rsi`=%s,`williams`=%s,`atr-14`=%s,`money-flow-index`=%s WHERE `pair_id`= (SELECT `id` From `currencies` WHERE `pair`=%s LIMIT 1)"
        # else:
        

        sql = "UPDATE `stock_params` SET `stoch_signal`=%s, `StochasticOscillator`=%s,`psar`=%s,`psar_down`=%s,`psar_down_indicator`=%s,`psar_up`=%s,`psar_up_indicator`=%s,`adx_positive`=%s, `adx_negative`=%s ,`ichimoku_a`=%s, `ichimoku_b`=%s , `ichimoku_base_line`=%s , `ichimoku_conversion_line`=%s , `historical_low`=%s,`historical_high`=%s,`historical_low_date`=%s,`historical_high_date`=%s,`rsi`=%s,`macd`=%s,`Signal_Line`=%s,`MACD_Line`=%s,`uo`=%s,`roc`=%s,`ema_10`=%s,`ema_20`=%s,`ema_50`=%s,`ema_100`=%s,`ema_200`=%s,`sma_10`=%s,`sma_20`=%s,`sma_50`=%s,`sma_100`=%s,`sma_200`=%s,`stoch`=%s,`adx`=%s,`cci_20`=%s,`chaikin_money_flow`=%s,`stoch_rsi`=%s,`williams`=%s,`atr_14`=%s,`money_flow_index`=%s WHERE `Inscode`=%s"
        

        # mydb = mysql.connector.connect(user = self.mysqluser, host = self.mySqlHost, database = self.mySqlDBName)
        # mycursor = mydb.cursor()
        # r=mycursor.execute(sql, val)
        # mydb.commit()
        val = (
            float(symbols_return['StochasticOscillator']['value']['stoch_signal']) if not math.isnan(symbols_return['StochasticOscillator']['value']['stoch_signal']) and not math.isinf(symbols_return['StochasticOscillator']['value']['stoch_signal']) else 0,
            float(symbols_return['StochasticOscillator']['value']['StochasticOscillator']) if not math.isnan(symbols_return['StochasticOscillator']['value']['StochasticOscillator']) and not math.isinf(symbols_return['StochasticOscillator']['value']['StochasticOscillator']) else 0,

            float(symbols_return['PSAR']['value']['psar']) if not math.isnan(symbols_return['PSAR']['value']['psar']) and not math.isinf(symbols_return['PSAR']['value']['psar']) else 0,
            float(symbols_return['PSAR']['value']['psar_down']) if not math.isnan(symbols_return['PSAR']['value']['psar_down']) and not math.isinf(symbols_return['PSAR']['value']['psar_down']) else 0,
            float(symbols_return['PSAR']['value']['psar_down_indicator']) if not math.isnan(symbols_return['PSAR']['value']['psar_down_indicator']) and not math.isinf(symbols_return['PSAR']['value']['psar_down_indicator']) else 0,
            float(symbols_return['PSAR']['value']['psar_up']) if not math.isnan(symbols_return['PSAR']['value']['psar_up']) and not math.isinf(symbols_return['PSAR']['value']['psar_up']) else 0,
            float(symbols_return['PSAR']['value']['psar_up_indicator']) if not math.isnan(symbols_return['PSAR']['value']['psar_up_indicator']) and not math.isinf(symbols_return['PSAR']['value']['psar_up_indicator']) else 0,
            
            float(symbols_return['adx']['value']['adx_positive']) if not math.isnan(symbols_return['adx']['value']['adx_positive']) and not math.isinf(symbols_return['adx']['value']['adx_positive']) else 0,
            float(symbols_return['adx']['value']['adx_negative']) if not math.isnan(symbols_return['adx']['value']['adx_negative']) and not math.isinf(symbols_return['adx']['value']['adx_negative']) else 0,
            
            float(symbols_return['ichimoku']['value']['ichimoku_a']) if not math.isnan(symbols_return['ichimoku']['value']['ichimoku_a']) and not math.isinf(symbols_return['ichimoku']['value']['ichimoku_a']) else 0,
            float(symbols_return['ichimoku']['value']['ichimoku_b']) if not math.isnan(symbols_return['ichimoku']['value']['ichimoku_b']) and not math.isinf(symbols_return['ichimoku']['value']['ichimoku_b']) else 0,
            float(symbols_return['ichimoku']['value']['ichimoku_base_line']) if not math.isnan(symbols_return['ichimoku']['value']['ichimoku_base_line']) and not math.isinf(symbols_return['ichimoku']['value']['ichimoku_base_line']) else 0,
            float(symbols_return['ichimoku']['value']['ichimoku_conversion_line'])  if not math.isnan(symbols_return['ichimoku']['value']['ichimoku_conversion_line']) and not math.isinf(symbols_return['ichimoku']['value']['ichimoku_conversion_line']) else 0,
             
        
            float(symbols_return['historical_price']['value']['low']) if not math.isnan(symbols_return['historical_price']['value']['low']) and not math.isinf(symbols_return['historical_price']['value']['low']) else 0,
            float(symbols_return['historical_price']['value']['high']) if not math.isnan(symbols_return['historical_price']['value']['high']) and not math.isinf(symbols_return['historical_price']['value']['high']) else 0,
            (symbols_return['historical_price']['value']['date_low']),
            (symbols_return['historical_price']['value']['date_high']),
            float(symbols_return['rsi']['value']['RSI']) if not math.isnan(symbols_return['rsi']['value']['RSI']) and not math.isinf(symbols_return['rsi']['value']['RSI']) else 0,
            
            float(symbols_return['macd']['value']['MACD-Histogram']) if not math.isnan(symbols_return['macd']['value']['MACD-Histogram']) and not math.isinf(symbols_return['macd']['value']['MACD-Histogram']) else 0,
            float(symbols_return['macd']['value']['Signal-Line']) if not math.isnan(symbols_return['macd']['value']['Signal-Line']) and not math.isinf(symbols_return['macd']['value']['Signal-Line']) else 0,
            float(symbols_return['macd']['value']['MACD-Line']) if not math.isnan(symbols_return['macd']['value']['MACD-Line']) and not math.isinf(symbols_return['macd']['value']['MACD-Line']) else 0,
            
            float(symbols_return['uo']['value']['uo']) if not math.isnan(symbols_return['uo']['value']['uo']) and not math.isinf(symbols_return['uo']['value']['uo']) else 0, 
            float(symbols_return['roc']['value']['roc']) if not math.isnan(symbols_return['roc']['value']['roc']) and not math.isinf(symbols_return['roc']['value']['roc']) else 0,
            float(symbols_return['ema-10']['value']) if not math.isnan(symbols_return['ema-10']['value']) and not math.isinf(symbols_return['ema-10']['value']) else 0,
            float(symbols_return['ema-20']['value']) if not math.isnan(symbols_return['ema-20']['value']) and not math.isinf(symbols_return['ema-20']['value']) else 0,
            float(symbols_return['ema-50']['value']) if not math.isnan(symbols_return['ema-50']['value']) and not math.isinf(symbols_return['ema-50']['value']) else 0,
            float(symbols_return['ema-100']['value']) if not math.isnan(symbols_return['ema-100']['value']) and not math.isinf(symbols_return['ema-100']['value']) else 0,
            float(symbols_return['ema-200']['value']) if not math.isnan(symbols_return['ema-200']['value']) and not math.isinf(symbols_return['ema-200']['value']) else 0, 
            float(symbols_return['sma-10']['value']) if not math.isnan(symbols_return['sma-10']['value']) and not math.isinf(symbols_return['sma-10']['value']) else 0,
            float(symbols_return['sma-20']['value']) if not math.isnan(symbols_return['sma-20']['value']) and not math.isinf(symbols_return['sma-20']['value']) else 0,
            float(symbols_return['sma-50']['value']) if not math.isnan(symbols_return['sma-50']['value']) and not math.isinf(symbols_return['sma-50']['value']) else 0,
            float(symbols_return['sma-100']['value']) if not math.isnan(symbols_return['sma-100']['value']) and not math.isinf(symbols_return['sma-100']['value']) else 0,
            float(symbols_return['sma-200']['value']) if not math.isnan(symbols_return['sma-200']['value']) and not math.isinf(symbols_return['sma-200']['value']) else 0, 
            float(symbols_return['stoch']['value']['stoch']) if not math.isnan(symbols_return['stoch']['value']['stoch']) and not math.isinf(symbols_return['stoch']['value']['stoch']) else 0, 
            float(symbols_return['adx']['value']['adx']) if not math.isnan(symbols_return['adx']['value']['adx']) and not math.isinf(symbols_return['adx']['value']['adx']) else 0, 
            float(symbols_return['cci-20']['value']['cci-20'])  if not math.isnan(symbols_return['cci-20']['value']['cci-20']) and not math.isinf(symbols_return['cci-20']['value']['cci-20']) else 0, 
            float(symbols_return['chaikin-money-flow']['value']['chaikin-money-flow'])  if not math.isnan(symbols_return['chaikin-money-flow']['value']['chaikin-money-flow']) else 0,
            float(symbols_return['stoch-rsi']['value']['stoch-rsi']) if not math.isnan(symbols_return['stoch-rsi']['value']['stoch-rsi']) and not math.isinf(symbols_return['stoch-rsi']['value']['stoch-rsi']) else 0, 
            float(symbols_return['williams']['value']['williams']) if not math.isnan(symbols_return['williams']['value']['williams']) and not math.isinf(symbols_return['williams']['value']['williams']) else 0,
            float(symbols_return['atr-14']['value']['atr-14']) if not math.isnan(symbols_return['atr-14']['value']['atr-14']) and not math.isinf(symbols_return['atr-14']['value']['atr-14']) else 0, 
            float(symbols_return['money-flow-index']['value']['MFI']) if not math.isnan(symbols_return['money-flow-index']['value']['MFI']) and not math.isinf(symbols_return['money-flow-index']['value']['MFI']) else 0,
            symbol
        )
        connection = self.connect_to_mysql()
        type(symbols_return['historical_price']['value']['date_low'])
        cursor = connection.cursor()
        cursor.execute(sql, val)
        connection.commit()
        try:
            if cursor.rowcount==0:
                
                sql ="INSERT INTO `stock_params` (`stoch_signal`,`StochasticOscillator`,`psar`,`psar_down`,`psar_down_indicator`,`psar_up`,`psar_up_indicator`,`adx_positive`, `adx_negative` ,`ichimoku_a`, `ichimoku_b`, `ichimoku_base_line`, `ichimoku_conversion_line`,`historical_low`,`historical_high`,`historical_low_date`,`historical_high_date`,`rsi`, `macd`,`Signal_Line`,`MACD_Line`, `uo`, `roc`, `ema_10`, `ema_20`, `ema_50`, `ema_100`, `ema_200`, `sma_10`, `sma_20`, `sma_50`, `sma_100`, `sma_200`, `stoch`, `adx`, `cci_20`, `chaikin_money_flow`, `stoch_rsi`, `williams`, `atr_14`, `money_flow_index`,`InsCode`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                r=cursor.execute(sql, val)
                connection.commit()
                print("Inserted")
            print(cursor.rowcount, "details inserted")
            pass
        except:
            pass
        cursor.close()
        connection.close()


class ScanicalIndicator:
    def PPSR(self,data):
        df = pd.to_datetime(data['date'])
        df.resample('1D').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'})

        PP = pd.Series((data['High'] + data['Low'] + data['Close']) / 3)
        R1 = pd.Series(2 * PP - data['Low'])
        S1 = pd.Series(2 * PP - data['High'])
        R2 = pd.Series(PP + data['High'] - data['Low'])
        S2 = pd.Series(PP - data['High'] + data['Low'])
        R3 = pd.Series(data['High'] + 2 * (PP - data['Low']))
        S3 = pd.Series(data['Low'] - 2 * (data['High'] - PP))
        psr = {'PP':PP, 'R1':R1, 'S1':S1, 'R2':R2, 'S2':S2, 'R3':R3, 'S3':S3}
        PSR = pd.DataFrame(psr)
        data= data.join(PSR)
        return data
    def VWAP(self,series_buffer):
        df=pd.DataFrame( series_buffer)
        df['date']=pd.to_datetime(df['date'])
        cutoff_date =df["date"].max().replace(hour=00, minute=00)
        df = df[df['date'] >= cutoff_date]
        df=df.sort_values(by='date', ascending=True)

        return (df.volume*(df.high+df.low+df.close)/3).cumsum() / df.volume.cumsum()

print('run service')


q=queue.Queue()

def now_time_run():
    now = datetime.now()
    time_tset_now = now.strftime("%H%M")
    weekday=datetime.today().weekday()
    print(time_tset_now)
    s_t = "1301"
    e_t = "1302"
    if time_tset_now <e_t and time_tset_now >= s_t and weekday in [6,5,0,1,2]:
        exit_run = False
    else:
        exit_run = True
    return exit_run

if __name__ == '__main__':
    try:
        indicator=IndicatorUpdate()
        #indicator.load_machines()
        while True:
            while now_time_run():
                print("Exit time")
                time.sleep(60)
        
            indicator.load_machines()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
