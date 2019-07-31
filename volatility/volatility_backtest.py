
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import plotly as plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots

df = pd.read_csv('ntprog_live_md1_MD_25_07.csv')

df=df.sort_values(by=['SampleTime'], ascending=True)
print(df.head())


# In[30]:


df['MidPx'] = (df['BidPx'] + df['AskPx'])/2

df['LogReturn'] = (np.log(df['MidPx']) - np.log(df['MidPx'].shift(1)))*100

short_wind = 20
long_wind = 300

vol_short = []
vol_long = []

for i in range(long_wind):
    vol_short.append(0)
    vol_long.append(0)

for i in range(len(df['MidPx'])-long_wind):
    vol_short.append(np.std(df['LogReturn'][long_wind-short_wind+i:long_wind+i]))
    vol_long.append(np.std(df['LogReturn'][i:long_wind+i]))
    
df['VolShort'] = vol_short    
df['VolLong'] = vol_long
print(df.tail())


# In[31]:


# Calculating volatility trigger levels
quantile_level1 = 0.85
quantile_level2 = 0.90
quantile_level3 = 0.95
quantile_level4 = 0.98

VolLong_trigger1 = df['VolLong'].quantile(quantile_level1)
VolShort_trigger1 = df['VolShort'].quantile(quantile_level1)

VolLong_trigger2 = df['VolLong'].quantile(quantile_level2)
VolShort_trigger2 = df['VolShort'].quantile(quantile_level2)

VolLong_trigger3 = df['VolLong'].quantile(quantile_level3)
VolShort_trigger3 = df['VolShort'].quantile(quantile_level3)

VolLong_trigger4 = df['VolLong'].quantile(quantile_level4)
VolShort_trigger4 = df['VolShort'].quantile(quantile_level4)

df['VolLong_trigger1'] = VolLong_trigger1
df['VolShort_trigger1'] = VolShort_trigger1

df['VolLong_trigger2'] = VolLong_trigger2
df['VolShort_trigger2'] = VolShort_trigger2

df['VolLong_trigger3'] = VolLong_trigger3
df['VolShort_trigger3'] = VolShort_trigger3

df['VolLong_trigger4'] = VolLong_trigger4
df['VolShort_trigger4'] = VolShort_trigger4


# Create figure with secondary y-axis
fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add rub prices and volatility
fig.add_trace(go.Scatter(x=df['SampleTime'], y=df['MidPx'], name="MidPx"), secondary_y=False,)

fig.add_trace(go.Scatter(x=df['SampleTime'], y=df['VolShort'], name="VolShort"), secondary_y=True)

fig.add_trace(go.Scatter(x=df['SampleTime'], y=df['VolLong'], name="VolLong"), secondary_y=True)

# Add volatiliy triggers
fig.add_trace(go.Scatter(x=df['SampleTime'], y=df['VolLong_trigger1'], name="VolLong_trigger1"), secondary_y=True)
fig.add_trace(go.Scatter(x=df['SampleTime'], y=df['VolShort_trigger1'], name="VolShort_trigger1"), secondary_y=True)



# Add figure title
fig.update_layout(
    title_text="USD/RUB price and volatility"
)

# Set x-axis title
fig.update_xaxes(title_text="time")

# Set y-axes titles
fig.update_yaxes(title_text="rub price", secondary_y=False)
fig.update_yaxes(title_text="volatility", secondary_y=True)


fig.show()

