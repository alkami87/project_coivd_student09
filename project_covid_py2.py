import os
import sys
import requests as r
import numpy as np
import pandas as pd
import datetime as dt
import psycopg2 as pg

# функция текущего времени
def now_str():
    return dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
print("[{}] project covid started".format(now_str()))
    
# функция строка подключения к PG
def setConStr():
    dbname = 'stock_exchange_s10'
    user = 'student10'
    password = 'student10_password'
    host = '87.242.126.7'
    return "dbname = {} user = {} password = {} host = {} port = 5432".format(dbname, user, password, host)
   
# функция вызова процедуры в PG
def callproc(sql_txt):
    try:
        con = pg.connect(setConStr())
        cur = con.cursor()
        cur.execute(sql_txt)
        con.commit() # -- в pg commit не нужен
        cur.close()
        con.close()
    except Exception as e:
        sys.exit('[{}] project covid ERROR \n Error text {}'.format(now_str(), e))

# Выгрузка данных из API    
print("[{}] project covid unload api".format(now_str()))
url = "https://covid-19.dataflowkit.com/v1"
data = r.get(url).json()

data = pd.DataFrame.from_dict(data, orient='columns')
data = data.reset_index()
data.columns = ['id', 'Active_Cases', 'Country', 'Last_Update'
                , 'New_Cases', 'New_Deaths', 'Total_Cases'
                , 'Total_Deaths', 'Total_Recovered']
data = data.set_index('id')

# Загрузка данных в STG таблицу в PG
print("[{}] project covid inserted stg in PostrgeSQL".format(now_str()))
report_dt = dt.datetime.today().strftime('%Y-%m-%d')
now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
for index, row in data.iterrows():
    f1 = now
    f2 = report_dt
    f3 = str(row['Active_Cases']).replace('+', '').replace(',', '').replace('N/A', '')
    f4 = str(row['Country']).upper()
    f5 = str(row['New_Cases']).replace('+', '').replace(',', '').replace('N/A', '')
    f6 = str(row['New_Deaths']).replace('+', '').replace(',', '').replace('N/A', '')
    f7 = str(row['Total_Cases']).replace('+', '').replace(',', '').replace('N/A', '')
    f8 = str(row['Total_Deaths']).replace('+', '').replace(',', '').replace('N/A', '')
    f9 = str(row['Total_Recovered']).replace('+', '').replace(',', '').replace('N/A', '')
    
    # построчная запись
    insert = """insert into project_coivd_student09.stg_covid_data select '{}'::timestamp(0), '{}'::date, '{}', '{}', '{}', '{}', '{}', '{}', '{}'"""
    insert = insert.format(f1, f2, f3, f4, f5, f6, f7, f8, f9)
    callproc(insert)

# вызов процедуры расчета
print("[{}] project covid call procedure in PostrgeSQL".format(now_str()))
proc = "call project_coivd_student09.proc_covid_calc()"
callproc(proc)

print("[{}] project covid finished".format(now_str()))
