# Databricks notebook source
# %sql
# CREATE SCHEMA IF NOT EXISTS dev.etl
# COMMENT 'Schema for DLT introduction'

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS dev.bronze.orders_raw DEEP CLONE samples.tpch.orders;
# CREATE TABLE IF NOT EXISTS dev.bronze.customer_raw DEEP CLONE samples.tpch.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1

# COMMAND ----------

import dlt

# COMMAND ----------

@dlt.table(
  table_properties={"quality" : "bronze"},
  comment = "Order bronze table"
)
def orders_broze():
    df = spark.readStream.table("dev.bronze.orders_raw")
    return df

# COMMAND ----------

@dlt.table(
  table_properties={"quality" : "bronze"},
  comment = "Customer bronze table",
  name = "customer_bronze"
)
def cust_broze():
    df = spark.read.table("dev.bronze.customer_raw")
    return df

# COMMAND ----------

@dlt.view(comment = "Joined view")
def joined_vw():
    df_c = spark.read.table("LIVE.customer_bronze")
    df_o = spark.read.table("LIVE.orders_broze")
    df_join = df_o.join(df_c, how = "left_outer", on = df_c.c_custkey == df_o.o_custkey)
    return df_join

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, count
@dlt.table(
  table_properties={"quality" : "silver"},
  comment = "Joined view",
  name = "joined_silver"
)
def joined_silver():
    df = spark.read.table("LIVE.joined_vw").withColumn("__insert_date", current_timestamp())
    return df

# COMMAND ----------

@dlt.table(
  table_properties={"quality" : "gold"},
  comment = "Orders aggregated gold",
  name = "orders_agg_gold"
)
def joined_gold():
    df = spark.read.table("LIVE.joined_silver")\
        .groupBy("c_mktsegment").agg(count("o_orderkey").alias("sum_orders"))\
        .withColumn("__insert_date", current_timestamp())
    return df
