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

import numpy as np
from scipy.stats import linregress


class TrendUpdate(threading.Thread):

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

            r = self.get_data(symbol)
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
            df['time'] = pd.to_datetime(df['time'])
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
                            xxmindate = np.append(xxmindate, df.iloc[i].time.timestamp())
                        if df.iloc[i].pivot == 2:
                            maxim = np.append(maxim, df.iloc[i].high)
                            xxmax = np.append(xxmax, i) # df.iloc[i].name
                            xxmaxdate = np.append(xxmaxdate, df.iloc[i].time.timestamp())
                            
                    if not np.any(xxmin) or not np.any(minim) or not np.any(xxmax) or not np.any(maxim):
                        continue
                               
                    #slmin, intercmin = np.polyfit(xxmin, minim,1) #numpy
                    #slmax, intercmax = np.polyfit(xxmax, maxim,1)

                    slmin, intercmin, rmin, pmin, semin = linregress(xxmin, minim)
                    slmax, intercmax, rmax, pmax, semax = linregress(xxmax, maxim)



                    #dfpl = df[candleid-backcandles-10:candleid+backcandles+10]
                    dfpl = df[-backcandles:]


                    xxmin = np.append(xxmin, xxmin[-1]+15)
                    xxmindate = np.append(xxmindate,  (df.iloc[-1].time+ pd.Timedelta(days=15)).timestamp())

                    xxmax = np.append(xxmax, xxmax[-1]+15)
                    xxmaxdate = np.append(xxmaxdate, (df.iloc[-1].time+ pd.Timedelta(days=15)).timestamp())                    

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
                            xxmindate = np.append(xxmindate, df.iloc[i].time.timestamp())
                        if df.iloc[i].pivot == 2:
                            maxim = np.append(maxim, df.iloc[i].high)
                            xxmax = np.append(xxmax, i) # df.iloc[i].name
                            xxmaxdate = np.append(xxmaxdate, df.iloc[i].time.timestamp())
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
                #print(response.text)
                time.sleep(1)
                continue
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
    
    def update_key(self,symbol,symbols_return):


        sql = "UPDATE `stock_patterns` SET `trend`=%s,`pivot`=%s WHERE `Inscode`=%s"

        mydb = mysql.connector.connect(user = self.mysqluser, host = self.mySqlHost, database = self.mySqlDBName)
        val = (
            str(symbols_return['trend']['value']) ,
            str(symbols_return['pivot']['value']) ,
            symbol
        )

        mycursor = mydb.cursor()
        #print(sql%(val))
        r=mycursor.execute(sql, val)
        mydb.commit()
        try:
            if(mycursor.rowcount==0):
                
                sql ="INSERT INTO `stock_patterns` (`trend`,`pivot`,`InsCode`) VALUES (%s,%s,%s)"
                r=mycursor.execute(sql, val)
                mydb.commit()
                print("Inserted")
            print(mycursor.rowcount, "details inserted")
        except:
            pass
        
        mydb.close()


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
    s_t = "1701"
    e_t = "1702"
    if time_tset_now <e_t and time_tset_now >= s_t and weekday in [6,5,0,1,2]:
        exit_run = False
    else:
        exit_run = True
    return exit_run

if __name__ == '__main__':
    try:
        indicator=TrendUpdate()
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
