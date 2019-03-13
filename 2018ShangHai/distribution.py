import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

"""
1. Typical Distribution
"""

train_set_Mn = pd.read_csv('Max_min2015.csv')
train_set_Mn = train_set_Mn.dropna().transpose()
train_set_Mn = train_set_Mn.drop(train_set_Mn.index[[0]])

time_scale = train_set_Mn.index
time_x =[]

for j in range(0,len(time_scale)):
    if j%12 == 0:
        time_x.append(time_scale[j])
    else:
        time_x.append(" ")
len(time_x)


plt.figure(1)
plt.boxplot(train_set_Mn, 0, '')
plt.xticks(range(0,288),time_x, rotation = 40)
plt.title('Max-min,2007 to 2015')

train_set_QV = pd.read_csv('QV2015.csv')
train_set_QV = train_set_QV.dropna().transpose()
train_set_QV = train_set_QV.drop(train_set_QV.index[[0]])

plt.figure(2)
plt.boxplot(train_set_QV, 0, '')
plt.xticks(range(0,288),time_x, rotation = 40)
plt.title('QV,2007 to 2015')

"""
2. Events
"""

events = pd.read_csv('fxStreet.csv')
events = events[events['Volatility'] == 3] # Pick the value of vol = 3
events = events[events['AllDay'] == False] # AllDay == False
events_USD = events[events['Currency'] == 'USD'] # USD Currency
events_EUR = events[events['Currency'] == 'EUR'] # EUR Currency
USD_name = events_USD['Name'].unique() # Unique Name
EUR_name = events_EUR['Name'].unique() # Unique Name

USD_list = []
for name in USD_name:
    count_ =  len(events_USD[events_USD['Name'] == name])
    if count_ >= 50:
        USD_list.append(name)

EUR_list = []
for name in EUR_name:
    count_ =  len(events_EUR[events_EUR['Name'] == name])
    if count_ >= 50:
        EUR_list.append(name)



