
# coding: utf-8

# In[6]:


import pandas as pd
import numpy as np
import plotly as plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class Volatility:
    def __init__(self):
        self.historical_data_source: str = 'ntprog_live_md1_MD_25_07.csv'
        self.short_wind: int = 20
        self.long_wind: int = 300
        self.quantile_level1: float = 0.80
        self.quantile_level2: float = 0.90
        self.quantile_level3: float = 0.95
        self.quantile_level4: float = 0.98
        self.VolLong_trigger1: float = 0.0
        self.VolLong_trigger2: float = 0.0
        self.VolLong_trigger3: float = 0.0
        self.VolLong_trigger4: float = 0.0
        self.VolShort_trigger1: float = 0.0
        self.VolShort_trigger2: float = 0.0
        self.VolShort_trigger3: float = 0.0
        self.VolShort_trigger4: float = 0.0
    
    def volatility_backtest(self):
        df = pd.read_csv(self.historical_data_source)
        df=df.sort_values(by=['SampleTime'], ascending=True)
        # Calculate mid-prices and log returns 
        df['MidPx'] = (df['BidPx'] + df['AskPx'])/2
        df['LogReturn'] = (np.log(df['MidPx']) - np.log(df['MidPx'].shift(1)))*100   #in percent
        # Calculate short and long volatility
        short_wind = self.short_wind
        long_wind = self.long_wind
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
        
        # Calculating volatility trigger levels
        quantile_level1 = self.quantile_level1
        quantile_level2 = self.quantile_level2
        quantile_level3 = self.quantile_level3
        quantile_level4 = self.quantile_level4

        self.VolLong_trigger1 = df['VolLong'].quantile(quantile_level1)
        self.VolShort_trigger1 = df['VolShort'].quantile(quantile_level1)

        self.VolLong_trigger2 = df['VolLong'].quantile(quantile_level2)
        self.VolShort_trigger2 = df['VolShort'].quantile(quantile_level2)

        self.VolLong_trigger3 = df['VolLong'].quantile(quantile_level3)
        self.VolShort_trigger3 = df['VolShort'].quantile(quantile_level3)

        self.VolLong_trigger4 = df['VolLong'].quantile(quantile_level4)
        self.VolShort_trigger4 = df['VolShort'].quantile(quantile_level4)

        df['VolLong_trigger1'] = self.VolLong_trigger1
        df['VolShort_trigger1'] = self.VolShort_trigger1

        df['VolLong_trigger2'] = self.VolLong_trigger2
        df['VolShort_trigger2'] = self.VolShort_trigger2

        df['VolLong_trigger3'] = self.VolLong_trigger3
        df['VolShort_trigger3'] = self.VolShort_trigger3

        df['VolLong_trigger4'] = self.VolLong_trigger4
        df['VolShort_trigger4'] = self.VolShort_trigger4
        
        return df
        
    def volatility_graph(self, df):
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
        fig.update_layout(title_text="USD/RUB price and volatility")
        # Set x-axis title
        fig.update_xaxes(title_text="time")
        # Set y-axes titles
        fig.update_yaxes(title_text="rub price", secondary_y=False)
        fig.update_yaxes(title_text="volatility", secondary_y=True)
        fig.show()
    
    def volatility_calc(self, market_data):
        log_return = []
        for i in range(len(market_data)-1):
            diff = np.log(market_data[i+1]) - np.log(market_data[i])
            log_return.append(diff)
    
    def volatility_trigger(self):
        pass
    
volatility = Volatility()
df = volatility.volatility_backtest()
volatility.volatility_graph(df)


# In[4]:


print(df.tail())


# In[8]:


VolLong_trigger1

