# Databricks notebook source
dbutils.widgets.text("path","")
dbutils.widgets.text("table_name","")

# COMMAND ----------

path = dbutils.widgets.get("path")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

spark \
.read \
.format("csv") \
.option("header",True) \
.load(f"{path}/{table_name}.csv") \
.write \
.mode("overwrite") \
.saveAsTable(f"lb.{table_name}") 
