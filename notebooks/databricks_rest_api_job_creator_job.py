# Databricks notebook source
pip install -r requirements.txt

# COMMAND ----------

# MAGIC %run ./constants

# COMMAND ----------

from job_creator import job_creator

# COMMAND ----------

file_system = "databrickslogs"
directory = "config"
file_name = "config.json"
notebook_path = """/Users/user/Job_Creator/Job_Creator_Job_Notebook""" 
schedule = "1 12 10 * * ?"

# COMMAND ----------

jc = job_creator(workspace,token)

# COMMAND ----------

jc.add_new_jobs(storage_name,storage_key,file_system,directory,file_name, cluster_id, notebook_path, schedule)
