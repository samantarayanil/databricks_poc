-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/databricks-datasets/online_retail/data-001/data.csv")

-- COMMAND ----------

DROP SCHEMA IF EXISTS dev.bronze CASCADE;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS dev;
CREATE CATALOG IF NOT EXISTS prod;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dev.bronze;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev.bronze.sales
AS
SELECT *
FROM read_files(
  "dbfs:/databricks-datasets/online_retail/data-001/data.csv",
  header => true,
  format => "csv"
  )

-- COMMAND ----------

DESCRIBE EXTENDED dev.bronze.sales

-- COMMAND ----------

SELECT * FROM dev.bronze.sales LIMIT 10;

-- COMMAND ----------

SHOW CREATE TABLE dev.bronze.sales

-- COMMAND ----------

DELETE FROM dev.bronze.sales
WHERE InvoiceNo = '536366'

-- COMMAND ----------

DESCRIBE HISTORY dev.bronze.sales

-- COMMAND ----------

SHOW SCHEMAS IN  dev

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.table('dev.bronze.sales').show(10)

-- COMMAND ----------

OPTIMIZE dev.bronze.sales

-- COMMAND ----------

DESCRIBE HISTORY dev.bronze.sales

-- COMMAND ----------

SELECT * FROM dev.bronze.sales@v0
WHERE InvoiceNo = '536365'

-- COMMAND ----------

VACUUM dev.bronze.sales RETAIN 168 HOURS DRY RUN;

-- COMMAND ----------

SELECT COUNT(1) FROM dev.bronze.sales

-- COMMAND ----------

CREATE VOLUME dev.bronze.sales_vol

-- COMMAND ----------

DESCRIBE VOLUME dev.bronze.sales_vol

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("/Volumes/dev/bronze/sales_vol/files")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/databricks-datasets/online_retail/data-001/data.csv", "dbfs:/Volumes/dev/bronze/sales_vol/files")