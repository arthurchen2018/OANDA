#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  8 19:29:29 2018

@author: hwt
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import sqlite3 

FileRead = 'fxStreet.csv'
DataSet = pd.read_csv(FileRead) 
        
def shift_time(unix):
    time = datetime.datetime.fromtimestamp(unix)
    return time
   
def Shift_time():
    Unix = DataSet['Timestamp']
    Time = list(map(shift_time, Unix))
    DataSet["Time"] = Time    
   
Shift_time()
#def shift_time(unix):
#    time = datetime.datetime.fromtimestamp(unix)
#    return time
#
#Unix = DataSet['Timestamp']
#Time = list(map(shift_time, Unix))
#DataSet["Time"] = Time
  
class Draw():
    
    def __init__(self, DataSet, Num):          
        self.Data = DataSet[DataSet.Volatility == 3]
        self.Num = Num
            
    def Data_Currency(self, currency):
        Data_currency = self.Data[self.Data.Currency == currency]
        Data_currency = Data_currency.sort_values(by='Timestamp')
        Data_currency = Data_currency.reset_index(drop=True)
        return Data_currency
   
    def Big_Sample_Name(self, Data):
        Name = Data['Name']
        Name_unique = list(Name.unique())
        Name_list = list(Name)
        Name_unique_list = {}
        Name_unique_list['Name'] = []
        Name_unique_list['Number'] = []
        Name_unique_list['Time'] = []
        Name_unique_list['Value'] = {}
        Name_unique_list['Value']['AC_ratio'] = []
        Name_unique_list['Value']['A'] = []
        Name_unique_list['Value']['C'] = []
        for name_unique in Name_unique:
            if Name_list.count(name_unique) >= self.Num:
                Name_unique_list['Name'].append(name_unique)
                Name_unique_list['Number'].append(Name_list.count(name_unique))
                sub_Data = Data[Data.Name == name_unique]
                sub_Data = sub_Data.reset_index(drop=True)
                Name_unique_list['Value']['AC_ratio'].append(np.array((sub_Data['Actual']-sub_Data['Consensus'])/
                                (sub_Data['Consensus'] + 0.01*sub_Data['Consensus'].max()) ))
                Name_unique_list['Value']['A'].append(np.array(sub_Data['Actual']))
                Name_unique_list['Value']['C'].append(np.array(sub_Data['Consensus']))  
                Name_unique_list['Time'].append(sub_Data['Timestamp'])                             
        return Name_unique_list
                    
    def draw_pic_ratio(self, currency):
        Data_currency = self.Big_Sample_Name(self.Data_Currency(currency))
        for i in range(len(Data_currency['Name'])):
            name = '(' + currency + ')---' + Data_currency['Name'][i] 
            value_AC = Data_currency['Value']['AC_ratio'][i]
            value_A = Data_currency['Value']['A'][i]
            value_C = Data_currency['Value']['C'][i]
            if len(value_AC) - np.isnan(value_AC).sum() >= self.Num:
                value_AC = value_AC[~np.isnan(value_AC)]
                plt.figure(figsize=(10,11))
                plt.subplot(2,1,1)
                plt.hist(value_AC,bins=20,edgecolor='black',color='royalblue')
                plt.xlabel('value')
                plt.title('Hist.' + str(i+1) + '---' + name)
                plt.subplot(2,1,2)
                plt.plot(value_A, '+', color='red', label='Actual')
                plt.plot(value_C, '.', color='darkblue', label='Consensus')
                plt.legend(loc=1)
                plt.xlabel('times')
                plt.title('Plot.' + str(i+1) + '---' + name)
                save_name = 'Hist&Plot.' + str(i+1) + '---' + name
                save_path = '/Users/hwt/Desktop/study group/pic_2018-05-08/ratio/'
                plt.savefig(save_path+save_name+'.png')
        
Dr = Draw(DataSet, 50)
USD = Dr.Data_Currency("USD")
USD_List = Dr.Big_Sample_Name(USD)
#A = USD_List['Value']['A'][2]
#C = USD_List['Value']['C'][2]
#AC = A - C

#Dr.draw_pic_ratio('USD')
#Dr.draw_pic_ratio('EUR')
USD_indicator = {}
for name in USD_List['Name']:
    USD_indicator[name]=[]

def time_interval(time_now, start=1800, end=1800):
    start_time = time_now - start
    end_time = time_now + end
    return start_time, end_time

import sqlite3   
def db_conn(dbpath):
    conn = sqlite3.connect(dbpath)
    with open('schema.sql', mode='r') as f:
        conn.cursor().executescript(f.read())
    return conn

def max_min(event_data):
    mid_price = 1/2 * (event_data['bid'] + event_data['ask'])
    max_price = mid_price.max()
    min_price = mid_price.min()
    return max_price - min_price

def QV(event_data):
    event_data1 = event_data.copy()
    event_data1['mid_price'] = 1/2 * (event_data1['bid'] + event_data1['ask'])
    QV = event_data1[['mid_price','unixtime']].groupby("unixtime").mean()
    QV_1 = (QV['mid_price'].diff())**2
    QV_1 = QV_1[~np.isnan(QV_1)]   
    QV_2 = sum(QV_1)/1800
    return QV_2

def Length(event_data):
    return len(event_data)

def spread(event_data):
    spread_value = (event_data['ask'] - event_data['bid']).mean()
    return spread_value
    

USD_indicator = {}
for name in USD_List['Name']:
    USD_indicator[name]=[]
     
dbpath = './data.db' 
db = db_conn(dbpath)
a=[]
for i in range(len(USD_List['Name'])):
    event_name = USD_List['Name'][i]
    time_now = USD_List['Time'][i]
    interval = list(map(time_interval, time_now))

    for j in range(len(interval)):
        sql = "SELECT id, symbol, time as unixtime, datetime(time, 'unixepoch') as time, bid, ask, tof "\
              "FROM EUR_USD where time >= %.0f and time <= %.0f" % interval[j]
        event_data = pd.read_sql(sql, db)
        front_data = event_data[event_data.unixtime<=time_now[j]]
        later_data = event_data[event_data.unixtime>=time_now[j]]
#        USD_indicator[event_name].append([max_min(front_data),max_min(later_data),max_min(later_data)/max_min(front_data)])
        if len(event_data) != 0:
            USD_indicator[event_name].append([spread(front_data),spread(later_data),spread(later_data)/spread(front_data)])
#            USD_indicator[event_name].append([Length(front_data),Length(later_data),Length(later_data)/Length(front_data)])
#            USD_indicator[event_name].append([QV(front_data),QV(later_data),QV(later_data)/QV(front_data)])

            
'''
['Retail Sales (MoM)',
 'Gross Domestic Product Annualized',
 'Nonfarm Payrolls',
 'Unemployment Rate',
 'Consumer Price Index (YoY)',
 'Consumer Price Index Ex Food & Energy (YoY)',
 'Core Personal Consumption Expenditure - Price Index (YoY)',
 'ISM Manufacturing PMI',
 'Retail Sales ex Autos (MoM)',
 'Fed Interest Rate Decision',
 'Durable Goods Orders',
 'FOMC Minutes']
'''
#A= np.array(USD_indicator['Unemployment Rate'])
#A= np.array(USD_indicator['Gross Domestic Product Annualized'])
for name in USD_List['Name']:
    A= np.array(USD_indicator[name])
    B = A[:,2]
    B = B[~np.isnan(B)]
    
    plt.figure(figsize=(10,11))
    plt.subplot(2,1,1)
    plt.title(name)
    plt.hist(B,bins=20,edgecolor='black',color='royalblue')
    plt.axvline(x=B.mean(),linestyle='--',color='red')
    plt.xlabel('ratio')
    
    plt.subplot(2,1,2)
    plt.plot(B, color='royalblue')
    plt.axhline(y=B.mean(),linestyle='--',color='red')
    plt.xlabel('value')
    
    save_name = name
    save_path = '/Users/hwt/Desktop/study group/pic_2018-05-08/indcator/'
    plt.savefig(save_path+save_name+'.png')
    
    
    
    
plt.hist(B, bins=20 ,edgecolor='black',color='royalblue')
plt.axvline(x=B.mean(),linestyle='--',color='red')

plt.plot(B)
plt.axhline(y=B.mean(),linestyle='--',color='red')

        
#ACF and PACF plots:
from statsmodels.tsa.stattools import acf, pacf
lag_acf = acf(B, nlags=50)
lag_pacf = pacf(B, nlags=50, method='ols')
#Plot ACF: 
plt.subplot(121) 
plt.plot(lag_acf,color='royalblue')
plt.axhline(y=0,linestyle='--',color='gray')
plt.axhline(y=-1.96/np.sqrt(len(B)),linestyle='--',color='gray')
plt.axhline(y=1.96/np.sqrt(len(B)),linestyle='--',color='gray')
plt.title('Autocorrelation Function')

#Plot PACF:
plt.subplot(122)
plt.plot(lag_pacf)
plt.axhline(y=0,linestyle='--',color='gray')
plt.axhline(y=-1.96/np.sqrt(len(B)),linestyle='--',color='gray')
plt.axhline(y=1.96/np.sqrt(len(B)),linestyle='--',color='gray')
plt.title('Partial Autocorrelation Function')
plt.tight_layout()






    
