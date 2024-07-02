# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/day.csv
# MAGIC

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database bike_db;
# MAGIC use database bike_db;

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv").show()

# COMMAND ----------

df.write.saveAsTable("bike")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bike

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------


