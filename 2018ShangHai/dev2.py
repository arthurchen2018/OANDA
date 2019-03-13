# -*- coding: utf-8 -*-
"""
Created on Thu May 10 09:34:55 2018

@author: TENG
"""

import time
import sqlite3
import pandas as pd
import gzip
import os
import numpy as np
import matplotlib.pyplot as plt

def QV(array_like):
    diff_ = array_like.diff()
    diff_[0] = 0
    return np.sqrt((diff_**2).sum())


def un_gz(file_name):
    """ungz zip file"""
    g_file = gzip.GzipFile(file_name, "r")
    return g_file


def un_gz_all(path, years, freq="1800s"):
    maxmin = pd.DataFrame(columns=list(pd.timedelta_range("00:00:00", "23:59:00", freq=freq)))
    maxmin2 = maxmin.copy()
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
                    df['mid'] = (df['bid'] + df['ask']) / 2
                    df.index = pd.to_datetime(df['time'], unit='s')

                    df1 = df.resample(freq, how={'mid': "ohlc"}).copy()
                    df2 = df['mid'].resample(freq).apply(QV).copy()

                    df1.index = df1.index - pd.Timestamp(df1.index[0].date())
                    df1 = df1.reindex(pd.timedelta_range("00:00:00", "23:59:00", freq=freq))

                    df2.index = df2.index - pd.Timestamp(df2.index[0].date())
                    df2 = df2.reindex(pd.timedelta_range("00:00:00", "23:59:00", freq=freq))

                    maxmin.ix[fname[0:10], :] = df1['mid']['high'] - df1['mid']['low']
                    maxmin2.ix[fname[0:10], :] = df2

                    print(maxmin)
                    print(maxmin2)
                except:
                    pass


    return [maxmin, maxmin2]

               # except:
                    # print(fname)
               #     pass


if __name__ == "__main__":
    path = "./EUR_USD"
    years = list(range(2007,2015))
    years = [str(k) for k in years]
    freq = "300s"
    m= un_gz_all(path, years, freq=freq)
    m[0].to_csv('Max_min2015.csv')
    m[1].to_csv('QV2015.csv')

