# Databricks notebook source
pip install -r requirements.txt

# COMMAND ----------

# MAGIC %run ./constants

# COMMAND ----------

import datetime;
import time
from query_history import query_history

# COMMAND ----------

pd = "2023-02-09"
pdts = int(time.mktime(datetime.datetime.strptime(pd, "%Y-%m-%d").timetuple())*1000)
cd = "2023-02-11"
cdts = int(time.mktime(datetime.datetime.strptime(cd, "%Y-%m-%d").timetuple())*1000)

# COMMAND ----------

col_lis = ['duration','query_start_time_ms','query_end_time_ms','warehouse_id','user_name','query_text']
file_system = "databrickslogs"
directory = "query_history"
file_name = f"""query_history_{str(pd).replace('-','_')}_{str(cd).replace('-','_')}"""

# COMMAND ----------

qh = query_history(token,workspace)
data_to_save = qh.get_history_from_api(pdts,cdts,col_lis)

if data_to_save is not None:
    qh.save_to_adls(data_to_save, storage_name, storage_key,file_system,directory,file_name)
else:
    print('no data to save!')
