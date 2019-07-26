
# coding: utf-8

# In[59]:


import MySQLdb
import datetime as datetime
import datetime

def roundTime(dt=None, roundTo=60):
   """Round a datetime object to any time lapse in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   if dt == None : dt = datetime.datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

sql_settings = {
	"sql_ip":"46.228.199.76",
	"sql_login":"gareev",
	"sql_pass":"Fx1234",
	"sql_db":"volatility"
}

def db_set_connect(sql_ip, sql_login, sql_pass, sql_db):
	try:
		conn = MySQLdb.connect(host=sql_ip, user=sql_login, passwd=sql_pass, db=sql_db, use_unicode=True, charset="utf8")
		return conn
	except MySQLdb.Error as err:
		print("Connection error: {}".format(err))
		conn.close()
        
def select_data(sql_conn, sql):
    cursor = sql_conn.cursor()
    cursor.execute(sql)
    market_data = cursor.fetchall()
    return market_data

def volatility_calc(market_data):
    usd_rub_mid = []
    for i in range(len(market_data)):
        #7 for mid-market
        usd_rub_mid.append(round(market_data[i][7], 5))

    usd_rub_diff = []
    for i in range(len(usd_rub_mid) - 1):
        diff = usd_rub_mid[i+1] - usd_rub_mid[i]
        usd_rub_diff.append(round(diff,6))

    usd_rub_std_dev = np.std(usd_rub_diff)
    return usd_rub_std_dev

def fetching_data(date_time, window):
    sql_conn = db_set_connect(sql_settings["sql_ip"], sql_settings["sql_login"], sql_settings["sql_pass"], sql_settings["sql_db"])
    now = date_time
    now = roundTime(now, roundTo=1)
    now_minus_delta = now - datetime.timedelta(seconds=window)
    now_minus_delta = roundTime(now_minus_delta,roundTo=1)
    sql = "select * from quotes where date_time between '" + str(now_minus_delta) +"'" + " and '" + str(now) + "' order by date_time asc"
    market_data = select_data(sql_conn, sql)
    return market_data

if __name__ == "__main__":
    short_window = 20
    long_window = 600
    now = datetime.datetime(2019, 7, 25, 13, 53, 10)
    
    market_data = fetching_data(now, short_window)
    usd_rub_vol_short = volatility_calc(market_data)
    
    market_data = fetching_data(now, long_window)
    usd_rub_vol_long = volatility_calc(market_data)
    
    print(usd_rub_vol_short)
    print(usd_rub_vol_long)
    
    




# In[56]:


len(market_data)


# In[24]:


market_data[0]


# In[25]:


print(now)
print(now_minus_delta)


# In[58]:


import numpy as np
import pandas as pd

usd_rub_mid = []
for i in range(len(market_data)):
    #7 for mid-market
    usd_rub_mid.append(round(market_data[i][7], 5))

usd_rub_diff = []
for i in range(len(usd_rub_mid) - 1):
    diff = usd_rub_mid[i+1] - usd_rub_mid[i]
    usd_rub_diff.append(round(diff,6))

usd_rub_std_dev = np.std(usd_rub_diff)
print(usd_rub_mid)
print(usd_rub_diff)
print(usd_rub_std_dev)

    
                


# In[55]:


len(usd_rub_mid)
len(usd_rub_diff)

