# Databricks notebook source
# %sql
# CREATE SCHEMA IF NOT EXISTS dev.etl
# COMMENT 'Schema for DLT introduction'

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS dev.bronze.orders_raw DEEP CLONE samples.tpch.orders;
# CREATE TABLE IF NOT EXISTS dev.bronze.customer_raw DEEP CLONE samples.tpch.customer;

# COMMAND ----------

import dlt

# COMMAND ----------

_order_status = spark.conf.get("order_status")

# COMMAND ----------

#Streaming table used check point to ingest data incrementaly.
@dlt.table(
  table_properties={"quality" : "bronze"},
  comment = "Order bronze table"
)
def orders_bronze():
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

@dlt.table(
  table_properties={"quality" : "bronze"},
  comment = "Customer bronze table",
  name = "orders_autoloader_bronze"
  )
def orders_autoloader_bronze():  
    df=(
        spark
        .readStream
        .format("cloudFiles")
        .option("skipChangeCommits", "true")
        .option("cloudFiles.format", "csv")
        .option("pathGlobFilter", "*.csv")
        .option("header", "true")
        .option("cloudFiles.schemaHints", "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice decimal(18,2), o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority integer, o_comment string")
        .option("cloudFiles.schemaLocation", "/Volumes/dev/etl/landing/autoloader/schemas/")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load("/Volumes/dev/etl/landing/files/")
    )
    return df

# COMMAND ----------

dlt.create_streaming_table("orders_union_bronze")

#Append Order

@dlt.append_flow(
    target = "orders_union_bronze"
)
def orders_delta_append():
    return(
        spark.readStream.option("skipChangeCommits", "true").table("LIVE.orders_bronze")
    )

#Append autoloader
@dlt.append_flow(
    target = "orders_union_bronze"
)
def orders_autooader_append():
    return(
        spark.readStream.option("skipChangeCommits", "true").table("LIVE.orders_autoloader_bronze")
    )

# COMMAND ----------

@dlt.view(comment = "Joined view")
def joined_vw():
    df_c = spark.read.table("LIVE.customer_bronze")
    df_o = spark.read.table("LIVE.orders_union_bronze")
    df_join = df_o.join(df_c, how = "left_outer", on = df_c.c_custkey == df_o.o_custkey)
    return df_join

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, count, col
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
        .groupBy("c_mktsegment").agg(count("o_orderkey").alias("count_orders"))\
        .withColumn("__insert_date", current_timestamp())
    return df 

# COMMAND ----------

for status in _order_status.split(","):
    @dlt.table(
    table_properties={"quality" : "gold"},
    comment = "Orders aggregated gold",
    name = f"orders_agg_{status}_gold"
    )
    def joined_gold():
        df = spark.read.table("LIVE.joined_silver")\
            .filter(f"o_orderstatus = '{status}'")\
            .groupBy("c_mktsegment").agg(count("o_orderkey").alias("count_orders"))\
            .withColumn("__insert_date", current_timestamp())
        return df 
