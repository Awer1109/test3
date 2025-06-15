# -*- coding: utf-8 -*-
"""
Created on Tue Mar  4 21:12:24 2025

@author: user
"""

##################################################
## 把資料從 SQLite資料庫讀出來
#################################################
import pandas as pd
import sqlite3
def readDataFromDB(contractName):
    dbName='shioaji.db' 
    conn = sqlite3.connect(dbName)
    df=pd.read_sql(
        'SELECT * FROM '+contractName
        ,conn)
    #df.index=df['time']
    #df.set_index('time', inplace=True)
    return df


df = readDataFromDB(contractName='stock_KBar_2330')  ## 'stock_KBar_2330', 'future_KBar_MXF', 'future_KBar_TXF', 'stock_KBar_0050'
### 將 'time' column 變為日期資料型態
df['time'] = pd.to_datetime(df.time) ##將df.time轉換成時間資料
### 設定 'time' column 為 Index
df = df.set_index('time')


##################################################
## 將1m K資料轉變為不同週期K資料
#################################################
#寫成函數
# def sjBarsToDf(sjBars):
#     dfBars = pd.DataFrame({**sjBars})#轉成dataframe
#     dfBars.index =pd.to_datetime(dfBars.ts)
#     dfBars=dfBars.groupby(dfBars.index).first()
#     return dfBars
def resampleKbars(kbars,period='1d'): ## '1d', '1h', '15min' 
    #kbars_out = pd.DataFrame(columns = ['time', 'open', 'high', 'low', 'close', 'volume',  'amount', 'product'])
    kbars_out = pd.DataFrame(columns = ['open', 'high', 'low', 'close', 'volume',  'amount', 'product'])
    # kbars_out['time'] = kbars['time'].resample(period).first() #區間第一筆資料為開盤(time)
    kbars_out['open'] = kbars['open'].resample(period).first() #區間第一筆資料為開盤(open)
    kbars_out['high'] = kbars['high'].resample(period).max() #區間最大值為最高(high)
    kbars_out['low']  = kbars['low'].resample(period).min() #區間最小值為最低(low)
    kbars_out['close'] = kbars['close'].resample(period).last() #區間最後一個值為收盤(close)
    kbars_out['volume'] = kbars['volume'].resample(period).sum() #區間所有成交量加總
    kbars_out['amount'] = kbars['amount'].resample(period).sum() #區間所有成交量加總
    kbars_out['product'] = kbars['product'].resample(period).first() #區間第一筆資料為商品名稱(product)
    kbars_out=kbars_out.dropna()
    return kbars_out

#df=sjBarsToDf(kbars_MXFR1)
df_resample=resampleKbars(df,period='1d')  ## '1d', '1h', '15min' 
print(df_resample)

## 將索引 (time) 轉為普通欄位(column)
df_resample = df_resample.reset_index()

## 輸出成外部檔案
df_resample.to_excel('kbars_1d_2330_2020-01-02_To_2025-03-04.xlsx')
#df_resample.to_excel('kbars_1d_MXF_2020-01-02_To_2025-03-04.xlsx')
# df_resample.to_excel('kbars_1m_MXFR1_2021-12-01_To_2021-12-30.xlsx')
# df_resample.to_excel('kbars_1d_MXFR1_2021-12-01_To_2021-12-30.xlsx')