from cgitb import reset
import json
from unittest import result
import boto3
import sys
import yfinance as yf
import ast
import time
import random
import datetime

STREAM_NAME = "StockData_Stream"
AWS_ACCESS_KEY_ID = 'AKIAZI7BEUVL7EFR37FV'
AWS_SECRET_ACCESS_KEY ='vc2j5b/xBOypSZzAkHlHP/v+YbDg3OQwINMB09nA'
# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream
class StockPriceIngestion:
    def __init__(self) -> None:
        pass
    kinesis = boto3.client('kinesis', region_name = "us-east-1",aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY) #Modify this line of code according to your requirement.

    today = datetime.date.today()
    yesterday = datetime.date.today() - datetime.timedelta(1)
    def Generate_Data(self):
        tickers = ["MSFT","MVIS","GOOG","SPOT","INO", "OCGN","ABML","RLLCF","JNJ","PSFE"]
        #tickers = ["MSFT"]
        attempt = 0 
        drop = [] 
        ohlc_intraday = {} 
        #ohlc_52WeeksData={}
        while len(tickers) != 0 and attempt <=5:
            tickers = [j for j in tickers if j not in drop]
            for i in range(len(tickers)):
                try:
                    ## Add code to pull the data for the stocks specified in the doc
                    data = yf.download(tickers[i], start= self.yesterday, end= self.today, interval = '1h' )
                    ohlc_intraday[tickers[i]] = data
                    ohlc_intraday[tickers[i]]['Symbol'] = tickers[i]
                    
                    ## Add additional code to call 'info' API to get 52WeekHigh and 52WeekLow refering this this link - https://pypi.org/project/yfinance/
                    info = Amazon = yf.Ticker(tickers[i])
                    #ohlc_52WeeksData[tickers[i]]={'Symbol':tickers[i],'fiftyTwoWeekHigh':info.info['fiftyTwoWeekHigh'],'fiftyTwoWeekLow':info.info['fiftyTwoWeekLow']}
                    ohlc_intraday[tickers[i]]['fiftyTwoWeekHigh'] = info.info['fiftyTwoWeekHigh']
                    ohlc_intraday[tickers[i]]['fiftyTwoWeekLow'] = info.info['fiftyTwoWeekLow']
                    drop.append(tickers[i])  
                except:
                    print(tickers[i]," :failed to fetch data...retrying")
                    continue
            attempt+=1

        data = []
        tickers = ohlc_intraday.keys() 
        for ticker in tickers:
            for i,row in ohlc_intraday[ticker].iterrows():
                stock_data=row.to_json()
                data.append(stock_data)
            #data.append(ohlc_52WeeksData[ticker])    
            
        return data

    ## Add your code here to push data records to Kinesis stream.
    def sendStockTrade(self):
        # values = bytearray(str(self.Generate_Data()),'utf-8')
        # for x in values: print(x)
        result = self.Generate_Data()
        for x in result:
            # key = json.loads(str(x))["Symbol"]
            key = ast.literal_eval(x)
            print(key["Symbol"])
            try:
                response = self.kinesis.put_record(
                    StreamName=STREAM_NAME,
                    Data=bytearray(x,'utf-8'),
                    PartitionKey=key["Symbol"])
                print(response)
            except BaseException as err:
                print("Error in posting record to kinesis: " + err)
         
            
    
if __name__ == "__main__":
    A = StockPriceIngestion()
    A.sendStockTrade()
    