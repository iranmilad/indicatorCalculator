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

import numpy as np
from scipy.stats import linregress
import psycopg2
from os import environ

DB_USER = environ.get("FOM_DB_USER", default='db_tseshow_user')
DB_PASS = environ.get("FOM_DB_PASSWORD", default='l8PDQGtKyMvynFb')
DB_HOST = environ.get("FOM_DB_HOST", default='87.107.172.173')
DB_PORT = environ.get("FOM_DB_PORT", default='6033')
DB_NAME = environ.get("FOM_DB_NAME", default='stockfeeder')

class TrendUpdate(threading.Thread):

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
   

    def pointpos(self,x):
        if x['pivot']==1:
            return x['low']-1e-3
        elif x['pivot']==2:
            return x['high']+1e-3
        else:
            return np.nan
    def pivotid(self,df1, l, n1, n2): #n1 n2 before and after candle l
        if l-n1 < 0 or l+n2 >= len(df1):
            return 0
        
        pividlow=1
        pividhigh=1
        for i in range(l-n1, l+n2+1):
            if(df1.low[l]>df1.low[i]):
                pividlow=0
            if(df1.high[l]<df1.high[i]):
                pividhigh=0
        if pividlow and pividhigh:
            return 3
        elif pividlow:
            return 1
        elif pividhigh:
            return 2
        else:
            return 0
    def calculate_indicator(self,symbols,patterns,interval):
        start_time = time.time()



        symbols_return={}

        for symbol in symbols:
            r = self.get_data_noavaran(symbol)

            #r = self.get_data(symbol)
            
            # try:
            if not symbol in r.keys():
                print("symbol is not exist in %s "%(symbol))
                continue
            price_data=r[symbol]
            series_buffer=price_data

            if len(series_buffer)==0:
                print ("Symbol %s is null in timeframe %s" %(symbol,interval))
                continue

            df =pd.DataFrame(series_buffer,columns =['Idx','time', 'open','high','low','close','volume'])       
            #df['time'] = pd.to_tim(df['time'])
            df.set_index('Idx')
            df.sort_values(by='time',inplace=True)
            #Check if NA values are in data
            df=df[df['volume']!=0]
            df.reset_index(drop=True, inplace=True)
            rows, columns = df.shape
            print("number of rows:", rows)
            df.isna().sum()

            if len(df)<=210:
                continue
            for pattern in patterns:

 
                #try:

                if pattern=="trend":
                    df['pivot'] = df.apply(lambda x: self.pivotid(df, x.name,3,3), axis=1)
                    df['pointpos'] = df.apply(lambda row: self.pointpos(row), axis=1)
                    
                    backcandles = 20

                    candleid = 100

                    maxim = np.array([])
                    minim = np.array([])
                    xxmin = np.array([])
                    xxmax = np.array([])
                    xxmindate = np.array([])
                    xxmaxdate = np.array([])

                    for i in range(rows-backcandles, rows):
                        if df.iloc[i].pivot == 1:
                            minim = np.append(minim, df.iloc[i].low)
                            xxmin = np.append(xxmin, i) #could be i instead df.iloc[i].name
                            xxmindate = np.append(xxmindate, df.iloc[i].time)
                        if df.iloc[i].pivot == 2:
                            maxim = np.append(maxim, df.iloc[i].high)
                            xxmax = np.append(xxmax, i) # df.iloc[i].name
                            xxmaxdate = np.append(xxmaxdate, df.iloc[i].time)
                            
                    if not np.any(xxmin) or not np.any(minim) or not np.any(xxmax) or not np.any(maxim):
                        continue
                               
                    #slmin, intercmin = np.polyfit(xxmin, minim,1) #numpy
                    #slmax, intercmax = np.polyfit(xxmax, maxim,1)

                    slmin, intercmin, rmin, pmin, semin = linregress(xxmin, minim)
                    slmax, intercmax, rmax, pmax, semax = linregress(xxmax, maxim)



                    #dfpl = df[candleid-backcandles-10:candleid+backcandles+10]
                    dfpl = df[-backcandles:]


                    xxmin = np.append(xxmin, xxmin[-1]+15)
                    xxmindate = np.append(xxmindate,  (df.iloc[-1].time+ timedelta(days=15).total_seconds()))

                    xxmax = np.append(xxmax, xxmax[-1]+15)
                    xxmaxdate = np.append(xxmaxdate, (df.iloc[-1].time+ timedelta(days=15).total_seconds()))                    

                    x=xxmin
                    y=slmin*xxmin + intercmin
                                      
                    time_min=xxmindate
                    price_min=y
                    
                    x=xxmax
                    y=slmax*xxmax + intercmax
                    time_max=xxmaxdate
                    price_max=y



                    symbols_return[pattern]={
                            "value":{
                                "time_min": time_min.tolist(),
                                "price_min": price_min.tolist(),
                                "time_max": time_max.tolist(),
                                "price_max": price_max.tolist(),
                                
                            },

                            "last_update": df.iloc[-1]["time"],
                            "last_price": df.iloc[-1]["close"],
                    }

                elif pattern=="pivot":
                    df['pivot'] = df.apply(lambda x: self.pivotid(df, x.name,10,10), axis=1)
                    df['pointpos'] = df.apply(lambda row: self.pointpos(row), axis=1)
                    
                    backcandles = 300



                    maxim = np.array([])
                    minim = np.array([])
                    xxmin = np.array([])
                    xxmax = np.array([])
                    xxmindate = np.array([])
                    xxmaxdate = np.array([])

                    for i in range(rows-backcandles, rows):
                        if df.iloc[i].pivot == 1:
                            minim = np.append(minim, df.iloc[i].low)
                            xxmin = np.append(xxmin, i) #could be i instead df.iloc[i].name
                            xxmindate = np.append(xxmindate, df.iloc[i].time)
                        if df.iloc[i].pivot == 2:
                            maxim = np.append(maxim, df.iloc[i].high)
                            xxmax = np.append(xxmax, i) # df.iloc[i].name
                            xxmaxdate = np.append(xxmaxdate, df.iloc[i].time)
                            #convert xxmaxdate without +e for example 1.6266528e+09 show 1626652800
                             
                    if not np.any(xxmin) or not np.any(minim) or not np.any(xxmax) or not np.any(maxim):
                        continue                            
                            
                            




                                      
                    time_min=xxmindate
                    price_min=minim

                    time_max=xxmaxdate
                    price_max=maxim



                    symbols_return[pattern]={
                            "value":{
                                "time_min": time_min.tolist(),
                                "price_min": price_min.tolist(),
                                "time_max": time_max.tolist(),
                                "price_max": price_max.tolist(),
                                
                            },

                            "last_update": df.iloc[-1]["time"],
                            "last_price": df.iloc[-1]["close"],
                    }

                else:
                    print("pattern is:", pattern)
                    continue
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
                diffgr =[]
                #print(response.text)
                time.sleep(60)
                break
        main_dict[Inscode]=[]
        counter=0
        for i in diffgr:
            if float(i['PriceMin'])<=0:
                continue
            InsCode = i['InsCode']
            re_json = {
                "Idx" : counter,
                "time": i['DEven'],
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
        response = requests.request("GET", url, headers=headers, data=payload)
        data= response.text
        data=json.loads(data)
        counter=0
        for bar in data: 
        
            re_json={
                "idx": counter,
                "time": datetime.strptime(bar["tradeDateGre"], "%Y-%m-%dT%H:%M:%S").timestamp(),
                "open": float(bar["openingAdjPrice"]),
                "high": float(bar["maxAdjPrice"]),
                "low" : float(bar["minAdjPrice"]),
                "close" : float(bar["lastAdjPrice"]),
                "volume" : float(bar["tradeValue"]), 
            } 
            if Inscode not in main_dict.keys():
                main_dict[Inscode] = []
            main_dict[Inscode].append(re_json)  
            counter=counter+1  
            
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
        
        patterns = ["trend","pivot"]

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

    def getNoavaranExchangeSymbols(self):
        url = "https://data3.nadpco.com/api/v3/BaseInfo/Companies"

        payload={}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        data= response.text
        Instrument=json.loads(data)
        main_dict={}

        list=[]
        for c in Instrument:
            main_dict[c["tseCode"]]=c 
    
        return main_dict
    
    
    def update_key(self,symbol,symbols_return):


        sql = "UPDATE `stock_patterns` SET `trend`=%s,`pivot`=%s WHERE `Inscode`=%s"
        if not 'trend' in symbols_return or not 'pivot' in symbols_return:
            return
        
        # mydb = mysql.connector.connect(user = self.mysqluser, host = self.mySqlHost, database = self.mySqlDBName)
        val = (
            str(symbols_return['trend']['value']) ,
            str(symbols_return['pivot']['value']) ,
            symbol
        )

        connection = self.connect_to_mysql()
        cursor = connection.cursor()
        cursor.execute(sql, val)
        try:
            if(cursor.rowcount==0):
                
                sql ="INSERT INTO `stock_patterns` (`trend`,`pivot`,`InsCode`) VALUES (%s,%s,%s)"
                r=cursor.execute(sql, val)
                connection.commit()
                print("Inserted")
            print(cursor.rowcount ,"details inserted")
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
    s_t = "1700"
    e_t = "1701"
 
    if time_tset_now <e_t and time_tset_now >= s_t and weekday in [6,5,0,1,2]:
        exit_run = False
    else:
        exit_run = True
    return exit_run

if __name__ == '__main__':

    indicator=TrendUpdate()
    #indicator.load_machines()
    try:
        
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
