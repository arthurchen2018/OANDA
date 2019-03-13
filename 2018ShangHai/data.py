# -*- coding: utf-8 -*-
"""
Created on Wed May  9 08:40:58 2018

@author: TENG
"""

import sqlite3
import pandas as pd
import gzip
import os
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt


train_set_Mn = pd.read_csv('QV-2007-2015-300s.csv')
train_set_Mn = train_set_Mn.set_index('Unnamed: 0',1)
train_set_Mn.ix['median'] = train_set_Mn.median()
median_nor = train_set_Mn.loc['median']
r = median_nor.values
ratio = r[1:]/r[0:-1]
ratio[0] = 1
index_ = median_nor.index[0:-1]
median_nor1 = pd.DataFrame(ratio,index = index_ , columns = ['median'])


def QV(array_like):
    diff_ = array_like.diff()
    diff_[0] = 0
    return np.sqrt((diff_**2).sum())


def un_gz(file_name):
    """ungz zip file"""
#    f_name = file_name.replace(".gz", "")
    g_file = gzip.GzipFile(file_name, "r")
    return g_file
#    open(f_name, "wb+").write(g_file.read())
#    g_file.close()
    
def un_gz_all(path, db):
    years = os.listdir(path)
    for y in years:
        cpath = os.path.join(path, y)
        months = os.listdir(cpath)
        for m in months:
            fpath = os.path.join(cpath, m)
            files = os.listdir(fpath)
            for fname in files:
                f = un_gz(os.path.join(fpath, fname))
                try:
                    df = pd.read_csv(f, header=None)
                    df.columns = ["symbol", "time", "bid", "ask", "tof"]
                    df.to_sql("EUR_USD", db, if_exists="append", index=False)
                except:
                    print(fname)
                    
def db_conn():
    conn = sqlite3.connect('D:\\Shanghai\\data.db')
    with open('schema.sql', mode='r') as f:
        conn.cursor().executescript(f.read())
    #conn = create_engine('sqlite:///F:/data.db')
    #with open('schema.sql', mode='r') as f:
    #    conn.execute(f.read())
    return conn
    
#if __name__ == "__main__":
#    path = "./EUR_USD"
#    db = db_conn()
#    un_gz_all(path, db)
    
#if __name__ == "__main__":
#    db = db_conn()
#    sql = "SELECT id, symbol, time as time_, datetime(time, 'unixepoch') as time, bid, ask, tof FROM EUR_USD limit 100"
#   x = pd.read_sql(sql, db)

# Read Events
# Filters:
# Vol =3 , All day == False, USD/EUR, Count >= 50

events = pd.read_csv('fxStreet.csv')
events = events[events['Volatility'] == 3] # Pick the value of vol = 3
events = events[events['AllDay'] == False] # AllDay == False
events_USD = events[(events['Currency'] == 'USD') | (events['Currency'] == 'EUR')] # USD Currency && EUR currency
events_USD['Name_'] = events['Name'].astype(str) +" "+ events_USD['Country'].astype(str)
USD_name = events_USD['Name_'].unique() # Unique Name

"""
USD_list2 = []
for n_ in USD_list:
    if n_ in USD_name:
        USD_list2.append(n_)
"""
USD_list = []
for name in USD_name:
    count_ =  len(events_USD[events_USD['Name_'] == name])
    if count_ >= 50:
        USD_list.append(name)

bins = 300
db = db_conn()

event_data = {}
data_col = ['event', 'ratio', 'time', 'actual', 'estimated']

for event in USD_list:
    event_table = events_USD[events_USD.Name_ == event].sort_values(by = ['Timestamp'])
    count_ = 0
    event_frame = pd.DataFrame(columns=data_col)
    for time in event_table.Timestamp.values:
        info = event_table[event_table.Timestamp == time]
        count_ += 1
        try:
            sql_before = "SELECT id, symbol, time as unixtime, datetime(time, 'unixepoch') as time, bid, ask, tof "\
                  "FROM EUR_USD where time >= %.0f and time <= %.0f" % (time-bins, time)
            event_b = pd.read_sql(sql_before, db)
            event_b['mid'] = 0.5*(event_b['ask'] + event_b['bid'])
            event_b['t_'] = pd.to_datetime(event_b['time'])
            b = QV(event_b['mid'])
            # event_b = event_b.set_index('t_',1)
            # vol_b = event_b.resamle('300s').apply(QV)
            # vol_b = event_b.resample('1800s', how = {'mid':'ohlc'})
            # b = (vol_b['mid']['high'] - vol_b['mid']['low'])[0]
            # b = (event_b.mid.diff()**2).sum()
            sql_after = "SELECT id, symbol, time as unixtime, datetime(time, 'unixepoch') as time, bid, ask, tof " \
                  "FROM EUR_USD where time >= %.0f and time <= %.0f" % (time, time+bins)

            event_a = pd.read_sql(sql_after, db)
            event_a['mid'] = 0.5*(event_a['ask'] + event_a['bid'])
            event_a['t_'] = pd.to_datetime(event_a['time'])
            a =  QV(event_a['mid'])
            # event_a = event_a.set_index('t_',1)
            # vol_a = event_a.resample('1800s', how = {'mid':'ohlc'})
            # b = (event_b.mid.diff()**2).sum()
            # a = (vol_a['mid']['high'] - vol_a['mid']['low'])[0]

            print(count_)

            index_ = str(datetime.utcfromtimestamp(time))
            event_frame.ix[index_,'event'] = event
            event_frame.ix[index_,'ratio'] = float(a/b)
            event_frame.ix[index_,'time'] = str(datetime.utcfromtimestamp(time))[11:]
            event_frame.ix[index_,'actual'] = float(info.Actual.values)
            event_frame.ix[index_,'estimated'] = float(info.Consensus.values)
        except:
            pass

    event_T = pd.merge(event_frame, median_nor1, left_on='time', right_index=True)
    event_T['excess_ratio'] = ((event_T['ratio'] / event_T['median']) - 1).astype(float)

    # plt.hist(event_T['Est'],bins = 40)
    event_data[event] = event_T




value = []
name = []
for n in USD_list:
    v_ = event_data[n]
    ave_ = v_['excess_ratio'].mean()
    value.append(ave_)
    name.append(n)
Aa = pd.DataFrame(np.array(value),index = name ,columns=['ave_event_Max_min'])
Aa.to_csv('USD_report_QV_1800.csv')


colors = ['red', 'tan', 'lime']

name1 = USD_list[4]
data1 = event_data[name1]
data1['diff'] = (data1['actual'] - data1['estimated']).astype(float)
data_plot1 = data1[['diff','excess_ratio']]
c1 = []
c2 = []
c3 = []
for j in range(len(data_plot1)):
    if abs(data_plot1['diff'][j]) <= 0.1:
        c1.append(data_plot1['excess_ratio'][j])
    elif data_plot1['diff'][j] < -0.1:
        c2.append(data_plot1['excess_ratio'][j])
    elif data_plot1['diff'][j] > 0.1:
        c3.append(data_plot1['excess_ratio'][j])
sample1 = [c1,c2,c3]
plt.hist(sample1, 10, histtype='bar',color=colors)
plt.title(name1)
plt.text(4, 17.5,'red: abs < 0.1',size = 'large')
plt.text(4, 17.0,'grey: actual - est > 0.1',size = 'large')
plt.text(4, 16.5,'lime: actual - est < -0.1',size = 'large')



name2 = USD_list[3]
data2 = event_data[name2]
data2['diff'] = (data2['actual'] - data2['estimated']).astype(float)
data_plot2 = data2[['diff','excess_ratio']]
c1 = []
c2 = []
c3 = []
for j in range(len(data_plot2)):
    if abs(data_plot2['diff'][j]) <= 50:
        c1.append(data_plot2['excess_ratio'][j])
    elif data_plot2['diff'][j] < -50:
        c2.append(data_plot2['excess_ratio'][j])
    elif data_plot2['diff'][j] > 50:
        c3.append(data_plot2['excess_ratio'][j])
sample2 = [c1,c2,c3]
plt.hist(sample2, 10, histtype='bar',color=colors)
plt.title(name2)
plt.text(4, 17.5,'red: abs < 50',size = 'large')
plt.text(4, 17.0,'grey: actual - est > 50',size = 'large')
plt.text(4, 16.5,'lime: actual - est < -50',size = 'large')







# Max - Min Spread
# QV
# Variance
# tick
# information bar
# stand err (trand ratio)
# ...














#pd.Timestamp(time.ctime(1199212236))


