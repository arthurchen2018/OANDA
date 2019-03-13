import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

"""
Daily Volatility ratio
"""

train_set_Mn = pd.read_csv('2007-2015-1800s.csv')
train_set_Mn = train_set_Mn.set_index('Unnamed: 0',1)
train_set_Mn.ix['median'] = train_set_Mn.median()
median_nor = train_set_Mn.loc['median']
r = median_nor.values
ratio = r[1:]/r[0:-1]
ratio[0] = 1
index_ = median_nor.index[0:-1]
median_nor1 = pd.DataFrame(ratio,index = index_ , columns = ['median'])




median_nor = pd.Series(median_value, index = list(train_set_Mn))
median_nor = pd.DataFrame(median_nor,columns = ['time'])
event_T = pd.merge(event_frame,median_nor,left_on = 'time', right_index = True)
event_T['Est'] = (event_T['ratio'] - event_T['time_y'])-1
event_T['Est'] = event_T['Est'].astype(float)
event_T['AC'] = (event_T['actual']-event_T['estimated']).astype(float)





plt.scatter(event_T['AC'],event_T['Est'])
plt.axvline(event_T['AC'].mean())
plt.axhline(event_T['Est'].mean())
"""


Events Volatility
"""



