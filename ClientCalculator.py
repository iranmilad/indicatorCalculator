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



    def get_database_number(self,interval):
            return 10
 
    def get_token(self,request)->str:
        url = "https://data3.nadpco.com/api/v2/Token"
        
        username = "FFV147110053"
        password = "DDYEcPqgHdgUeAS"
        
        payload={}
        now = datetime.datetime.now()
        #datetime.datetime.strptime(request['noavaran']['TokenEx'], '%y/%m/%d %H:%M:%S')
        
        if not request['noavaran']['TokenEx'] or now>request['noavaran']['TokenEx']:
            response = requests.request("POST", url, auth=(username, password), data=payload)
            data =response.text
            #data = r"{'token': '0D82D591DF32F6F4FC8B557BB6892A14F0D77E7168F95C86CF5154D75BB3D825F939E4E92C430824FFAB89BC1734786305CD6667A53BD8AD31F98558BC05BE0A'}"
            data= data.replace("\'", "\"")

            
            data =json.loads(data)
            
            print(data['token'])
            
            
            now = now + datetime.timedelta(hours=6)
            json_object = json.dumps({"token":data,"ex":now}, indent=4, sort_keys=True, default=str)
            with open("NoavaranToken.json", "w") as outfile:
                outfile.write(json_object)
                
            request['noavaran']['TokenEx'] = now
            request['noavaran']['Token']= data['token']
            return request['noavaran']['Token']
            
        else:
            return request['noavaran']['Token']                

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

                if pattern=="buyHead":
                    df['buyHead_1']=(df.iloc[-1]["Buy_I_Value"]) / df.iloc[-1]["Buy_Count_ClientI"]
                    df['buyHead_5']=(df.iloc[-5:]["Buy_I_Value"].sum()) / df.iloc[-5:]["Buy_Count_ClientI"].sum()
                    df['buyHead_10']=(df.iloc[-10:]["Buy_I_Value"].sum()) / df.iloc[-10:]["Buy_Count_ClientI"].sum()
                    df['buyHead_20']=(df.iloc[-20:]["Buy_I_Value"].sum()) / df.iloc[-20:]["Buy_Count_ClientI"].sum()
                    df['buyHead_50']=(df.iloc[-50:]["Buy_I_Value"].sum()) / df.iloc[-50:]["Buy_Count_ClientI"].sum()
                    df['buyHead_100']=(df.iloc[-100:]["Buy_I_Value"].sum()) / df.iloc[-100:]["Buy_Count_ClientI"].sum()
                    df['buyHead_200']=(df.iloc[-200:]["Buy_I_Value"].sum()) / df.iloc[-200:]["Buy_Count_ClientI"].sum()
                    
                    

                    previous_df['buyHead_1']=  (previous_df.iloc[-1]["Buy_I_Value"] / previous_df["Buy_Count_ClientI"])
                    previous_df['buyHead_5']=  (previous_df.iloc[-5:]["Buy_I_Value"].sum()) / previous_df[-5:]["Buy_Count_ClientI"].sum()
                    previous_df['buyHead_10']=  (previous_df.iloc[-10:]["Buy_I_Value"].sum()) / previous_df[-10:]["Buy_Count_ClientI"].sum()
                    previous_df['buyHead_20']=  (previous_df.iloc[-20:]["Buy_I_Value"].sum()) / previous_df[-20:]["Buy_Count_ClientI"].sum()
                    previous_df['buyHead_50']=  (previous_df.iloc[-50:]["Buy_I_Value"].sum()) / previous_df[-50:]["Buy_Count_ClientI"].sum()
                    previous_df['buyHead_100']=  (previous_df.iloc[-100:]["Buy_I_Value"].sum()) / previous_df[-100:]["Buy_Count_ClientI"].sum()
                    previous_df['buyHead_200']=  (previous_df.iloc[-200:]["Buy_I_Value"].sum()) / previous_df[-200:]["Buy_Count_ClientI"].sum()
                    
                    symbols_return[pattern]={
                            'value':{
                                '1':df.iloc[-1]['buyHead_1'],
                                '5':df.iloc[-1]['buyHead_5'],
                                '10':df.iloc[-1]['buyHead_10'],
                                '20':df.iloc[-1]['buyHead_20'],
                                '50':df.iloc[-1]['buyHead_50'],
                                '100':df.iloc[-1]['buyHead_100'],
                                '200':df.iloc[-1]['buyHead_200']
                                
                            },
                            'previous':{
                                '1':previous_df.iloc[-1]['buyHead_1'],
                                '5':previous_df.iloc[-1]['buyHead_5'],
                                '10':previous_df.iloc[-1]['buyHead_10'],
                                '20':previous_df.iloc[-1]['buyHead_20'],
                                '50':previous_df.iloc[-1]['buyHead_50'],
                                '100':previous_df.iloc[-1]['buyHead_100'],
                                '200':previous_df.iloc[-1]['buyHead_200']
                            },
                            'last_update': df.iloc[-1]['date'],
                            
                    }

                elif pattern=="sellHead":
                    df['sellHead_1']=(df.iloc[-1]["Sell_I_Value"]) / df.iloc[-1]["Sell_Count_ClientI"]
                    df['sellHead_5']=(df.iloc[-5:]["Sell_I_Value"].sum()) / df.iloc[-5:]["Sell_Count_ClientI"].sum()
                    df['sellHead_10']=(df.iloc[-10:]["Sell_I_Value"].sum()) / df.iloc[-10:]["Sell_Count_ClientI"].sum()
                    df['sellHead_20']=(df.iloc[-20:]["Sell_I_Value"].sum()) / df.iloc[-20:]["Sell_Count_ClientI"].sum()
                    df['sellHead_50']=(df.iloc[-50:]["Sell_I_Value"].sum()) / df.iloc[-50:]["Sell_Count_ClientI"].sum()
                    df['sellHead_100']=(df.iloc[-100:]["Sell_I_Value"].sum()) / df.iloc[-100:]["Sell_Count_ClientI"].sum()
                    df['sellHead_200']=(df.iloc[-200:]["Sell_I_Value"].sum()) / df.iloc[-200:]["Sell_Count_ClientI"].sum()
                    
                    

                    previous_df['sellHead_1']=  (previous_df.iloc[-1]["Sell_I_Value"] / previous_df["Sell_Count_ClientI"])
                    previous_df['sellHead_5']=  (previous_df.iloc[-5:]["Sell_I_Value"].sum()) / previous_df[-5:]["Sell_Count_ClientI"].sum()
                    previous_df['sellHead_10']=  (previous_df.iloc[-10:]["Sell_I_Value"].sum()) / previous_df[-10:]["Sell_Count_ClientI"].sum()
                    previous_df['sellHead_20']=  (previous_df.iloc[-20:]["Sell_I_Value"].sum()) / previous_df[-20:]["Sell_Count_ClientI"].sum()
                    previous_df['sellHead_50']=  (previous_df.iloc[-50:]["Sell_I_Value"].sum()) / previous_df[-50:]["Sell_Count_ClientI"].sum()
                    previous_df['sellHead_100']=  (previous_df.iloc[-100:]["Sell_I_Value"].sum()) / previous_df[-100:]["Sell_Count_ClientI"].sum()
                    previous_df['sellHead_200']=  (previous_df.iloc[-200:]["Sell_I_Value"].sum()) / previous_df[-200:]["Sell_Count_ClientI"].sum()
                    
                    symbols_return[pattern]={
                            'value':{
                                '1':df.iloc[-1]['sellHead_1'],
                                '5':df.iloc[-1]['sellHead_5'],
                                '10':df.iloc[-1]['sellHead_10'],
                                '20':df.iloc[-1]['sellHead_20'],
                                '50':df.iloc[-1]['sellHead_50'],
                                '100':df.iloc[-1]['sellHead_100'],
                                '200':df.iloc[-1]['sellHead_200']
                                
                            },
                            'previous':{
                                '1':previous_df.iloc[-1]['sellHead_1'],
                                '5':previous_df.iloc[-1]['sellHead_5'],
                                '10':previous_df.iloc[-1]['sellHead_10'],
                                '20':previous_df.iloc[-1]['sellHead_20'],
                                '50':previous_df.iloc[-1]['sellHead_50'],
                                '100':previous_df.iloc[-1]['sellHead_100'],
                                '200':previous_df.iloc[-1]['sellHead_200']
                            },
                            'last_update': df.iloc[-1]['date'],
                            
                    }

                elif pattern=="powerBuy":

                    
                    df['powerBuy_1']=(df.iloc[-1]["Buy_I_Value"] / df.iloc[-1]["Buy_Count_ClientI"])/(df.iloc[-1]["Sell_I_Value"] / df.iloc[-1]["Sell_Count_ClientI"]) if df.iloc[-1]["Sell_Count_ClientI"]!=0 else 0
                    df['powerBuy_5']=(df.iloc[-5:]["Buy_I_Value"].sum() / df.iloc[-5:]["Buy_Count_ClientI"].sum())/(df.iloc[-5:]["Sell_I_Value"].sum() / df.iloc[-5:]["Sell_Count_ClientI"].sum()) if df.iloc[-5:]["Sell_Count_ClientI"].sum()!=0 else 0
                    df['powerBuy_10']=(df.iloc[-10:]["Buy_I_Value"].sum() / df.iloc[-10:]["Buy_Count_ClientI"].sum())/(df.iloc[-10:]["Sell_I_Value"].sum() / df.iloc[-10:]["Sell_Count_ClientI"].sum()) if df.iloc[-10:]["Sell_Count_ClientI"].sum()!=0 else 0
                    df['powerBuy_20']=(df.iloc[-20:]["Buy_I_Value"].sum() / df.iloc[-20:]["Buy_Count_ClientI"].sum())/(df.iloc[-20:]["Sell_I_Value"].sum() / df.iloc[-20:]["Sell_Count_ClientI"].sum()) if df.iloc[-20:]["Sell_Count_ClientI"].sum()!=0 else 0
                    df['powerBuy_50']=(df.iloc[-50:]["Buy_I_Value"].sum() / df.iloc[-50:]["Buy_Count_ClientI"].sum())/(df.iloc[-50:]["Sell_I_Value"].sum() / df.iloc[-50:]["Sell_Count_ClientI"].sum()) if df.iloc[-50:]["Sell_Count_ClientI"].sum()!=0 else 0
                    df['powerBuy_100']=(df.iloc[-100:]["Buy_I_Value"].sum() / df.iloc[-100:]["Buy_Count_ClientI"].sum())/(df.iloc[-100:]["Sell_I_Value"].sum() / df.iloc[-100:]["Sell_Count_ClientI"].sum()) if df.iloc[-100:]["Sell_Count_ClientI"].sum()!=0 else 0
                    df['powerBuy_200']=(df.iloc[-200:]["Buy_I_Value"].sum() / df.iloc[-200:]["Buy_Count_ClientI"].sum())/(df.iloc[-200:]["Sell_I_Value"].sum() / df.iloc[-200:]["Sell_Count_ClientI"].sum()) if df.iloc[-200:]["Sell_Count_ClientI"].sum()!=0 else 0
                    
                    previous_df['powerBuy_1']=  (previous_df.iloc[-1]["Buy_I_Value"] / previous_df.iloc[-1]["Buy_Count_ClientI"])/(previous_df.iloc[-1]["Sell_I_Value"] / previous_df.iloc[-1]["Sell_Count_ClientI"]) if previous_df.iloc[-1]["Sell_Count_ClientI"]!=0 else 0
                    previous_df['powerBuy_5']=  (previous_df.iloc[-5:]["Buy_I_Value"].sum() / previous_df.iloc[-5:]["Buy_Count_ClientI"].sum())/(previous_df.iloc[-5:]["Sell_I_Value"].sum() / previous_df.iloc[-5:]["Sell_Count_ClientI"].sum()) if previous_df.iloc[-5:]["Sell_Count_ClientI"].sum()!=0 else 0
                    previous_df['powerBuy_10']=  (previous_df.iloc[-10:]["Buy_I_Value"].sum() / previous_df.iloc[-10:]["Buy_Count_ClientI"].sum())/(previous_df.iloc[-10:]["Sell_I_Value"].sum() / previous_df.iloc[-10:]["Sell_Count_ClientI"].sum()) if previous_df.iloc[-10:]["Sell_Count_ClientI"].sum()!=0 else 0
                    previous_df['powerBuy_20']=  (previous_df.iloc[-20:]["Buy_I_Value"].sum() / previous_df.iloc[-20:]["Buy_Count_ClientI"].sum())/(previous_df.iloc[-20:]["Sell_I_Value"].sum() / previous_df.iloc[-20:]["Sell_Count_ClientI"].sum()) if previous_df.iloc[-20:]["Sell_Count_ClientI"].sum()!=0 else 0
                    previous_df['powerBuy_50']=  (previous_df.iloc[-50:]["Buy_I_Value"].sum() / previous_df.iloc[-50:]["Buy_Count_ClientI"].sum())/(previous_df.iloc[-50:]["Sell_I_Value"].sum() / previous_df.iloc[-50:]["Sell_Count_ClientI"].sum()) if previous_df.iloc[-50:]["Sell_Count_ClientI"].sum()!=0 else 0
                    previous_df['powerBuy_100']=  (previous_df.iloc[-100:]["Buy_I_Value"].sum() / previous_df.iloc[-100:]["Buy_Count_ClientI"].sum())/(previous_df.iloc[-100:]["Sell_I_Value"].sum() / previous_df.iloc[-100:]["Sell_Count_ClientI"].sum()) if previous_df.iloc[-100:]["Sell_Count_ClientI"].sum()!=0 else 0
                    previous_df['powerBuy_200']=  (previous_df.iloc[-200:]["Buy_I_Value"].sum() / previous_df.iloc[-200:]["Buy_Count_ClientI"].sum())/(previous_df.iloc[-200:]["Sell_I_Value"].sum() / previous_df.iloc[-200:]["Sell_Count_ClientI"].sum()) if previous_df.iloc[-200:]["Sell_Count_ClientI"].sum()!=0 else 0
                    
                    
                    symbols_return[pattern]={
                            'value':{
                                '1':df.iloc[-1]['powerBuy_1'],
                                '5':df.iloc[-1]['powerBuy_5'],
                                '10':df.iloc[-1]['powerBuy_10'],
                                '20':df.iloc[-1]['powerBuy_20'],
                                '50':df.iloc[-1]['powerBuy_50'],
                                '100':df.iloc[-1]['powerBuy_100'],
                                '200':df.iloc[-1]['powerBuy_200'],
                                  
                                
                            },
                            'previous':{
                                '1':previous_df.iloc[-1]['powerBuy_1'],
                                '5':previous_df.iloc[-1]['powerBuy_5'],
                                '10':previous_df.iloc[-1]['powerBuy_10'],
                                '20':previous_df.iloc[-1]['powerBuy_20'],
                                '50':previous_df.iloc[-1]['powerBuy_50'],
                                '100':previous_df.iloc[-1]['powerBuy_100'],
                                '200':previous_df.iloc[-1]['powerBuy_200'],
                            },
                            'last_update': df.iloc[-1]['date'],
                    }

                elif pattern=="moneyEnter":
                    df['moneyEnter_1']=(df.iloc[-1]["Buy_I_Value"] - df.iloc[-1]["Sell_I_Value"])
                    df['moneyEnter_5']=(df.iloc[-5:]["Buy_I_Value"].sum() - df.iloc[-5:]["Sell_I_Value"].sum())
                    df['moneyEnter_10']=(df.iloc[-10:]["Buy_I_Value"].sum() - df.iloc[-10:]["Sell_I_Value"].sum())
                    df['moneyEnter_20']=(df.iloc[-20:]["Buy_I_Value"].sum() - df.iloc[-20:]["Sell_I_Value"].sum())
                    df['moneyEnter_50']=(df.iloc[-50:]["Buy_I_Value"].sum() - df.iloc[-50:]["Sell_I_Value"].sum())
                    df['moneyEnter_100']=(df.iloc[-100:]["Buy_I_Value"].sum() - df.iloc[-100:]["Sell_I_Value"].sum())
                    df['moneyEnter_200']=(df.iloc[-200:]["Buy_I_Value"].sum() - df.iloc[-200:]["Sell_I_Value"].sum())
                    
                    previous_df['moneyEnter_1']=(previous_df.iloc[-1]["Buy_I_Value"] - previous_df.iloc[-1]["Sell_I_Value"])
                    previous_df['moneyEnter_5']=(previous_df.iloc[-5:]["Buy_I_Value"].sum() - previous_df.iloc[-5:]["Sell_I_Value"].sum())
                    previous_df['moneyEnter_10']=(previous_df.iloc[-10:]["Buy_I_Value"].sum() - previous_df.iloc[-10:]["Sell_I_Value"].sum())
                    previous_df['moneyEnter_20']=(previous_df.iloc[-20:]["Buy_I_Value"].sum() - previous_df.iloc[-20:]["Sell_I_Value"].sum())
                    previous_df['moneyEnter_50']=(previous_df.iloc[-50:]["Buy_I_Value"].sum() - previous_df.iloc[-50:]["Sell_I_Value"].sum())
                    previous_df['moneyEnter_100']=(previous_df.iloc[-100:]["Buy_I_Value"].sum() - previous_df.iloc[-100:]["Sell_I_Value"].sum())
                    previous_df['moneyEnter_200']=(previous_df.iloc[-200:]["Buy_I_Value"].sum() - previous_df.iloc[-200:]["Sell_I_Value"].sum())
                    
                    symbols_return[pattern]={
                            'value':{
                                '1':df.iloc[-1]['moneyEnter_1'],
                                '5':df.iloc[-1]['moneyEnter_5'],
                                '10':df.iloc[-1]['moneyEnter_10'],
                                '20':df.iloc[-1]['moneyEnter_20'],
                                '50':df.iloc[-1]['moneyEnter_50'],
                                '100':df.iloc[-1]['moneyEnter_100'],
                                '200':df.iloc[-1]['moneyEnter_200'],
                                  
                                
                            },
                            'previous':{
                                '1':previous_df.iloc[-1]['moneyEnter_1'],
                                '5':previous_df.iloc[-1]['moneyEnter_5'],
                                '10':previous_df.iloc[-1]['moneyEnter_10'],
                                '20':previous_df.iloc[-1]['moneyEnter_20'],
                                '50':previous_df.iloc[-1]['moneyEnter_50'],
                                '100':previous_df.iloc[-1]['moneyEnter_100'],
                                '200':previous_df.iloc[-1]['moneyEnter_200'],
                            },
                            'last_update': df.iloc[-1]['date'],
                    }
                    
                     




            if symbols_return:

                self.update_key(symbol,symbols_return)
                print("%s updated" %symbol)
            else:
                print('symbols is none')


        print("%s --- %s seconds ---" % (time.strftime("%a, %d %b %Y %H:%M:%S", time.gmtime()),(time.time() - start_time)))

    def get_data(self,Inscode):
        main_dict = {}
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
        <ClientTypeByInsCode xmlns="http://tsetmc.com/">
        <UserName>{username}</UserName>
        <Password>{password}</Password>
        <InsCode>{int(Inscode)}</InsCode>
        </ClientTypeByInsCode>
        </soap12:Body>
        </soap12:Envelope>"""     
        while True:
            try:
                response = requests.post(url, headers=headers, data=payload)
                all = xmltodict.parse(response.text)
                diffgr = all['soap:Envelope']['soap:Body']['ClientTypeByInsCodeResponse']['ClientTypeByInsCodeResult']['diffgr:diffgram']['Data']['Data']
                break
            except:
                diffgr =[]
                time.sleep(60)
                break
        main_dict[Inscode]=[]
        if not isinstance(diffgr, list):
            return main_dict
        for i in diffgr:

            InsCode = i['InsCode']
     
            
            re_json = {
                "date": i['RecDate'],
                "Buy_I_Volume": float(i['Buy_I_Volume']),
                "Buy_N_Volume": float(i['Buy_N_Volume']),
                "Buy_I_Value": float(i['Buy_I_Value']),
                "Buy_N_Value": float(i['Buy_N_Value']),
                "Buy_Count_ClientN": float(i['Buy_Count_ClientN']),
                "Buy_Count_ClientI": float(i['Buy_Count_ClientI']),
                "Sell_I_Volume": float(i['Sell_I_Volume']),
                "Sell_N_Volume": float(i['Sell_N_Volume']),
                "Sell_I_Value": float(i['Sell_I_Value']),
                "Sell_N_Value": float(i['Sell_N_Value']),
                "Sell_Count_ClientN": float(i['Sell_Count_ClientN']),
                "Sell_Count_ClientI": float(i['Sell_Count_ClientI']),
                
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
        
        patterns = ["buyHead","sellHead","powerBuy","moneyEnter"]

        allSymbols=self.getSymbols()
        
        for symbols in zip(*(iter(allSymbols),) * 20):
            self.calculate_indicator(symbols,patterns,"1d")
            print('sleep')
            time.sleep(60)
            
       
    def getExchangeSymbols(self,exchange):
        url = 'https://feed.tseshow.com/api/stockInSector'
        #url = 'http://localhost:8000/api/stockInSector'
        
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
    
    def update_key(self,symbol,symbols_return):
        # if symbol in self.crypto_symbol:
        #     sql = "UPDATE `crypto_indicators` SET `rsi`=%s,`macd`=%s,`uo`=%s,`roc`=%s,`ema-10`=%s,`ema-20`=%s,`ema-50`=%s,`ema-100`=%s,`ema-200`=%s,`sma-10`=%s,`sma-20`=%s,`sma-50`=%s,`sma-100`=%s,`sma-200`=%s,`stoch`=%s,`adx`=%s,`cci-20`=%s,`chaikin-money-flow`=%s,`stoch-rsi`=%s,`williams`=%s,`atr-14`=%s,`money-flow-index`=%s WHERE `crypto_id`= (SELECT `id` From `cryptos` WHERE `symbol`=%s LIMIT 1)"
        # elif symbol in self.forex_symbol:
        #     sql = "UPDATE `currency_indicators` SET `rsi`=%s,`macd`=%s,`uo`=%s,`roc`=%s,`ema-10`=%s,`ema-20`=%s,`ema-50`=%s,`ema-100`=%s,`ema-200`=%s,`sma-10`=%s,`sma-20`=%s,`sma-50`=%s,`sma-100`=%s,`sma-200`=%s,`stoch`=%s,`adx`=%s,`cci-20`=%s,`chaikin-money-flow`=%s,`stoch-rsi`=%s,`williams`=%s,`atr-14`=%s,`money-flow-index`=%s WHERE `pair_id`= (SELECT `id` From `currencies` WHERE `pair`=%s LIMIT 1)"
        # else:
        

        sql = "UPDATE `stock_clients` SET `buyHead_1`=%s,`buyHead_5`=%s, `buyHead_10`=%s, `buyHead_20`=%s, `buyHead_50`=%s , `buyHead_100`=%s, `buyHead_200`=%s , `sellHead_1`=%s, `sellHead_5`=%s, `sellHead_10`=%s, `sellHead_20`=%s, `sellHead_50`=%s, `sellHead_100`=%s, `sellHead_200`=%s, `powerBuy_1`=%s, `powerBuy_5`=%s, `powerBuy_10`=%s, `powerBuy_20`=%s, `powerBuy_50`=%s, `powerBuy_100`=%s, `powerBuy_200`=%s, `moneyEnter_1`=%s, `moneyEnter_5`=%s, `moneyEnter_10`=%s, `moneyEnter_20`=%s, `moneyEnter_50`=%s, `moneyEnter_100`=%s, `moneyEnter_200`=%s"
        sql = sql + " WHERE `InsCode`=%s"
        connection = self.connect_to_mysql()
        val = (
            float(symbols_return['buyHead']['value']['1']) if not math.isnan(symbols_return['buyHead']['value']['1']) and not math.isinf(symbols_return['buyHead']['value']['1']) else 0,
            float(symbols_return['buyHead']['value']['5']) if not math.isnan(symbols_return['buyHead']['value']['5']) and not math.isinf(symbols_return['buyHead']['value']['5']) else 0,
            float(symbols_return['buyHead']['value']['10']) if not math.isnan(symbols_return['buyHead']['value']['10']) and not math.isinf(symbols_return['buyHead']['value']['10']) else 0,
            float(symbols_return['buyHead']['value']['20']) if not math.isnan(symbols_return['buyHead']['value']['20']) and not math.isinf(symbols_return['buyHead']['value']['20']) else 0,
            float(symbols_return['buyHead']['value']['50']) if not math.isnan(symbols_return['buyHead']['value']['50']) and not math.isinf(symbols_return['buyHead']['value']['50']) else 0,
            float(symbols_return['buyHead']['value']['100']) if not math.isnan(symbols_return['buyHead']['value']['100']) and not math.isinf(symbols_return['buyHead']['value']['100']) else 0,
            float(symbols_return['buyHead']['value']['200']) if not math.isnan(symbols_return['buyHead']['value']['200']) and not math.isinf(symbols_return['buyHead']['value']['200']) else 0,
            float(symbols_return['sellHead']['value']['1']) if not math.isnan(symbols_return['sellHead']['value']['1']) and not math.isinf(symbols_return['sellHead']['value']['1']) else 0,
            float(symbols_return['sellHead']['value']['5']) if not math.isnan(symbols_return['sellHead']['value']['5']) and not math.isinf(symbols_return['sellHead']['value']['5']) else 0,
            float(symbols_return['sellHead']['value']['10']) if not math.isnan(symbols_return['sellHead']['value']['10']) and not math.isinf(symbols_return['sellHead']['value']['10']) else 0,
            float(symbols_return['sellHead']['value']['20']) if not math.isnan(symbols_return['sellHead']['value']['20']) and not math.isinf(symbols_return['sellHead']['value']['20']) else 0,
            float(symbols_return['sellHead']['value']['50']) if not math.isnan(symbols_return['sellHead']['value']['50']) and not math.isinf(symbols_return['sellHead']['value']['50']) else 0,
            float(symbols_return['sellHead']['value']['100']) if not math.isnan(symbols_return['sellHead']['value']['100']) and not math.isinf(symbols_return['sellHead']['value']['100']) else 0,
            float(symbols_return['sellHead']['value']['200']) if not math.isnan(symbols_return['sellHead']['value']['200']) and not math.isinf(symbols_return['sellHead']['value']['200']) else 0,
            float(symbols_return['powerBuy']['value']['1']) if not math.isnan(symbols_return['powerBuy']['value']['1']) and not math.isinf(symbols_return['powerBuy']['value']['1']) else 0,    
            float(symbols_return['powerBuy']['value']['5']) if not math.isnan(symbols_return['powerBuy']['value']['5']) and not math.isinf(symbols_return['powerBuy']['value']['5']) else 0,    
            float(symbols_return['powerBuy']['value']['10']) if not math.isnan(symbols_return['powerBuy']['value']['10']) and not math.isinf(symbols_return['powerBuy']['value']['10']) else 0,
            float(symbols_return['powerBuy']['value']['20']) if not math.isnan(symbols_return['powerBuy']['value']['20']) and not math.isinf(symbols_return['powerBuy']['value']['20']) else 0,
            float(symbols_return['powerBuy']['value']['50']) if not math.isnan(symbols_return['powerBuy']['value']['50']) and not math.isinf(symbols_return['powerBuy']['value']['50']) else 0,
            float(symbols_return['powerBuy']['value']['100']) if not math.isnan(symbols_return['powerBuy']['value']['100']) and not math.isinf(symbols_return['powerBuy']['value']['100']) else 0,
            float(symbols_return['powerBuy']['value']['200']) if not math.isnan(symbols_return['powerBuy']['value']['200']) and not math.isinf(symbols_return['powerBuy']['value']['200']) else 0,
            float(symbols_return['moneyEnter']['value']['1']) if not math.isnan(symbols_return['moneyEnter']['value']['1']) and not math.isinf(symbols_return['moneyEnter']['value']['1']) else 0,
            float(symbols_return['moneyEnter']['value']['5']) if not math.isnan(symbols_return['moneyEnter']['value']['5']) and not math.isinf(symbols_return['moneyEnter']['value']['5']) else 0,
            float(symbols_return['moneyEnter']['value']['10']) if not math.isnan(symbols_return['moneyEnter']['value']['10']) and not math.isinf(symbols_return['moneyEnter']['value']['10']) else 0,
            float(symbols_return['moneyEnter']['value']['20']) if not math.isnan(symbols_return['moneyEnter']['value']['20']) and not math.isinf(symbols_return['moneyEnter']['value']['20']) else 0,
            float(symbols_return['moneyEnter']['value']['50']) if not math.isnan(symbols_return['moneyEnter']['value']['50']) and not math.isinf(symbols_return['moneyEnter']['value']['50']) else 0,
            float(symbols_return['moneyEnter']['value']['100']) if not math.isnan(symbols_return['moneyEnter']['value']['100']) and not math.isinf(symbols_return['moneyEnter']['value']['100']) else 0,
            float(symbols_return['moneyEnter']['value']['200']) if not math.isnan(symbols_return['moneyEnter']['value']['200']) and not math.isinf(symbols_return['moneyEnter']['value']['200']) else 0,     
                
                
            symbol
        )

        cursor = connection.cursor()
        cursor.execute(sql, val)
        connection.commit()
        try:
            if cursor.rowcount==0:
                # sql ="INSERT INTO `stock_params` (`stoch_signal`,`StochasticOscillator`,`psar`,`psar_down`,`psar_down_indicator`,`psar_up`,`psar_up_indicator`,`adx_positive`, `adx_negative` ,`ichimoku_a`, `ichimoku_b`, `ichimoku_base_line`, `ichimoku_conversion_line`,`historical_low`,`historical_high`,`historical_low_date`,`historical_high_date`,`rsi`, `macd`,`Signal_Line`,`MACD_Line`, `uo`, `roc`, `ema_10`, `ema_20`, `ema_50`, `ema_100`, `ema_200`, `sma_10`, `sma_20`, `sma_50`, `sma_100`, `sma_200`, `stoch`, `adx`, `cci_20`, `chaikin_money_flow`, `stoch_rsi`, `williams`, `atr_14`, `money_flow_index`,`InsCode`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                sql ="INSERT INTO `stock_clients` (`buyHead_1`, `buyHead_5`, `buyHead_10`, `buyHead_20`, `buyHead_50`, `buyHead_100`, `buyHead_200`,`sellHead_1`, `sellHead_5`, `sellHead_10`, `sellHead_20`, `sellHead_50`, `sellHead_100`, `sellHead_200`, `powerBuy_1`, `powerBuy_5`, `powerBuy_10`, `powerBuy_20`, `powerBuy_50`, `powerBuy_100`, `powerBuy_200`, `moneyEnter_1`, `moneyEnter_5`, `moneyEnter_10`, `moneyEnter_20`, `moneyEnter_50`, `moneyEnter_100`, `moneyEnter_200` ,`InsCode`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                r=cursor.execute(sql, val)
                connection.commit()
                print("Inserted")
            print(cursor.rowcount, "details inserted")
        except:
            pass
        cursor.close()
        connection.close()


print('run service')


q=queue.Queue()

def now_time_run():
    now = datetime.now()
    time_tset_now = now.strftime("%H%M")
    weekday=datetime.today().weekday()
    print(time_tset_now)
    s_t = "1531"
    e_t = "1532"
    if time_tset_now <e_t and time_tset_now >= s_t and weekday in [6,5,0,1,2]:
        exit_run = False
    else:
        exit_run = True
    return exit_run

if __name__ == '__main__':
    try:
        
        
        indicator=IndicatorUpdate()
        indicator.load_machines()
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
