
# coding: utf-8

# In[49]:


import MySQLdb
import datetime as datetime
import numpy as np

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
        
def run_query(sql_conn, sql):
    cursor = sql_conn.cursor()
    cursor.execute(sql)
    market_data = cursor.fetchall()
    return market_data

def volatility_calc(market_data):
    usd_rub_mid = []
    for i in range(len(market_data)):
        #7 for mid-market
        usd_rub_mid.append(round(market_data[i][7], 5))
    #usd_rub_diff = []
    #for i in range(len(usd_rub_mid) - 1):
    #    diff = (usd_rub_mid[i+1] - usd_rub_mid[i])
    #    usd_rub_diff.append(diff)
    usd_rub_std_dev = np.std(usd_rub_mid)
    return [round(usd_rub_std_dev, 6), round(market_data[-1][7], 6)]

def fetching_data(date_time, window):
    now = date_time
    now = roundTime(now, roundTo=1)
    now_minus_delta = now - datetime.timedelta(seconds=window)
    now_minus_delta = roundTime(now_minus_delta,roundTo=1)
    sql = "select * from quotes where date_time between '" + str(now_minus_delta) +"'" + " and '" + str(now) + "' order by date_time asc"
    market_data = run_query(sql_conn, sql)
    return market_data

def inserting_data(sql_conn, short_window, long_window, mid, vol_short, vol_long, date_time):
    sql = "insert into volcount (windshort, windlong, mid, volshort, vollong, date_time) values ('" + str(short_window) + "', '" + str(long_window) + "', '" + str(mid) + "', '" + str(vol_short) + "', '" + str(vol_long) + "', '" + str(date_time) +"')"
    run_query(sql_conn, sql)
    print("Volatility data saved in database")
    sql_conn.commit()
    return 0
    
if __name__ == "__main__":
    
    sql_conn = db_set_connect(sql_settings["sql_ip"], sql_settings["sql_login"], sql_settings["sql_pass"], sql_settings["sql_db"])
    
    short_window = 30
    long_window = 300
    start_time = datetime.datetime(2019, 7, 25, 14, 15, 0)
    end_time = datetime.datetime(2019, 7, 25, 14, 25, 0)
    time_delta = end_time - start_time
    time_delta = time_delta.total_seconds()
    time_range = []
    #frequency of volatlity calculation
    frequency_coefficient = 1
    for i in range(int(time_delta)):
        time_var = start_time + datetime.timedelta(seconds =i*frequency_coefficient)
        time_range.append(time_var)
    
    vol_calculated = []
    for time in time_range:
        now = time
        market_data = fetching_data(now, short_window)
        [usd_rub_vol_short, usd_rub_mid] = volatility_calc(market_data)
        
        market_data = fetching_data(now, long_window)
        [usd_rub_vol_long, usd_rub_mid] = volatility_calc(market_data)
        
        inserting_data(sql_conn, short_window, long_window, usd_rub_mid, usd_rub_vol_short, usd_rub_vol_long, now)
        
        vol_calculated.append((now, usd_rub_vol_short, usd_rub_vol_long))
        
    print(vol_calculated)
    
    




# In[41]:


market_data[-1]


# In[46]:


market_data[-3]


# In[43]:


print(now)
print(now_minus_delta)


# In[ ]:


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

    
                


# In[ ]:


len(usd_rub_mid)
len(usd_rub_diff)

