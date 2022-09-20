# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## DATA SOURCE: Segment table
# MAGIC ## STAGE: Raw --> Bronze
# MAGIC ##### Description: This notebook reads raw files from landing zone in s3 and converts the raw data into a schema, and inserts into a Bronse layer table(This is simply raw data injested into Delta table which conforms to the required schema). 
# MAGIC 
# MAGIC To run example code change the config values in *configuration* folder. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Incremental Data Ingestion with Auto Loader
# MAGIC 
# MAGIC Incremental ETL is important since it allows us to deal solely with new data that has been encountered since the last ingestion. Reliably processing only the new data reduces redundant processing and helps enterprises reliably scale data pipelines.
# MAGIC 
# MAGIC The first step for any successful data lakehouse implementation is ingesting into a Delta Lake table from cloud storage. 
# MAGIC 
# MAGIC Historically, ingesting files from a data lake into a database has been a complicated process.
# MAGIC 
# MAGIC Databricks Auto Loader provides an easy-to-use mechanism for incrementally and efficiently processing new data files as they arrive in cloud file storage. In this notebook, you'll see Auto Loader in action.
# MAGIC 
# MAGIC Due to the benefits and scalability that Auto Loader delivers, Databricks recommends its use as general **best practice** when ingesting data from cloud object storage.
# MAGIC 
# MAGIC ## Dataset Used
# MAGIC This demo uses simplified artificially generated medical data representing heart rate recordings delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # 1. load config

# COMMAND ----------

import os
#load config from the configuration file
cwd = os.getcwd()
config_file = f"{cwd}/configuration/config.json"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

#parametrize noteboook to showcase how to templatize the code for future using widgets.
#This will allow passing 2 parameters dynamically to this notebook the etl_stage and the table_name.
#we will store the config of the autoloader for a particular table as best practice into a separate config file.
dbutils.widgets.text("table_name", "table_name")
dbutils.widgets.text("etl_stage", "raw_to_bronze")

# COMMAND ----------

stage = dbutils.widgets.get("etl_stage")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

with open(config_file, "r") as config_file:
    config = json.load(config_file)
    
## Input File Path
data_source = config.get(stage).get(table_name).get("params").get("inputFilePath")

## Checkpoint Location s3://<bucket>/<stage>/<entity>/<dataSource>/<destination>/<version> 
checkpoint_location = config.get(stage).get(table_name).get("params").get("checkpointLocation")

## Not parameterized as it should be consistent across data sources and SHOULD break if it changes
autoloader_schema_location = config.get(stage).get(table_name).get("params").get("autoloaderSchemaLocation")

## Decide to start over Yes/No
start_over = config.get(stage).get(table_name).get("params").get("startOver")

if start_over.lower() not in ["no", "yes"]:
    startOver = "no"

## Trigger Window
trigger_window = config.get(stage).get(table_name).get("params").get("triggerConfig")

print(f"Loading from {data_source}")
print(f"With checkpoint: {checkpoint_location}")
print(f"Starting Over: {start_over}")

# COMMAND ----------

if start_over.lower() == "yes":
    
    ## Delete checkpoints to reprocess all data
    dbutils.fs.rm(checkpoint_location, recurse=True)
    
    ## re-infer schema if changed
    dbutils.fs.rm(autoloader_schema_location, recurse=True)
    
    ##Delete table files and truncate table
    #dbutils.fs.rm(output_s3_path, recurse=True) ## Streaming Job will not create table DDL
    spark.sql(f"""TRUNCATE TABLE {config_name}""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 2.  Using Auto Loader
# MAGIC 
# MAGIC In the cell below, a function is defined to demonstrate using Databricks Auto Loader with the PySpark API. This code includes both a Structured Streaming read and write.
# MAGIC 
# MAGIC The following notebook will provide a more robust overview of Structured Streaming. If you wish to learn more about Auto Loader options, refer to the <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">documentation</a>.
# MAGIC 
# MAGIC Note that when using Auto Loader with automatic <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html" target="_blank">schema inference and evolution</a>, the 4 arguments shown here should allow ingestion of most datasets. These arguments are explained below.
# MAGIC 
# MAGIC | argument | what it is | how it's used |
# MAGIC | --- | --- | --- |
# MAGIC | **`data_source`** | The directory of the source data | Auto Loader will detect new files as they arrive in this location and queue them for ingestion; passed to the **`.load()`** method |
# MAGIC | **`source_format`** | The format of the source data |  While the format for all Auto Loader queries will be **`cloudFiles`**, the format of the source data should always be specified for the **`cloudFiles.format`** option |
# MAGIC | **`table_name`** | The name of the target table | Spark Structured Streaming supports writing directly to Delta Lake tables by passing a table name as a string to the **`.table()`** method. Note that you can either append to an existing table or create a new table |
# MAGIC | **`checkpoint_directory`** | The location for storing metadata about the stream | This argument is pass to the **`checkpointLocation`** and **`cloudFiles.schemaLocation`** options. Checkpoints keep track of streaming progress, while the schema location tracks updates to the fields in the source dataset |
# MAGIC 
# MAGIC **NOTE**: The code below has been streamlined to demonstrate Auto Loader functionality. We'll see in later lessons that additional transformations can be applied to source data before saving them to Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Raw data Ingestion patterns using autoloader.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1.1) ingesting raw data using autoloader as a **stream**

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .table(table_name))
    return query

# COMMAND ----------

query = autoload_to_table(data_source = f"{content_s3_path}",
                          source_format = "json",
                          table_name = f"{table_name}",
                          checkpoint_directory = f"{checkpoint_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Because Auto Loader uses Spark Structured Streaming to load data incrementally, the code above doesn't appear to finish executing.
# MAGIC 
# MAGIC We can think of this as a **continuously active query**. This means that as soon as new data arrives in our data source, it will be processed through our logic and loaded into our target table.
# MAGIC 
# MAGIC If you want to run the autoloader on a schedule and not as stream which is up all the time use the __trigger__ config.
# MAGIC 
# MAGIC  **`trigger(availableNow=True)`** is very similar to **`trigger(once=True)`** but can run
# MAGIC multiple batches until all available data is consumed instead of once big batch and is introduced in
# MAGIC <a href="https://spark.apache.org/releases/spark-release-3-3-0.html" target="_blank">Spark 3.3.0</a> and
# MAGIC <a href="https://docs.databricks.com/release-notes/runtime/10.4.html" target="_blank">Databricks Runtime 10.4 LTS</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1.2 ingesting all available new raw data in one big batch.

# COMMAND ----------

def autoload_to_table_trigger_once(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .trigger(once=True)
                  .table(table_name))
    return query

# COMMAND ----------

query = autoload_to_table_trigger_once(data_source = f"{content_s3_path}",
                          source_format = "json",
                          table_name = f"{table_name}",
                          checkpoint_directory = f"{checkpoint_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1.2 ingesting all available new raw data in multiple batches.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing to Multiple Tables
# MAGIC 
# MAGIC Those familiar with Structured Streaming may be aware that the **`foreachBatch`** method provides the option to execute custom data writing logic on each microbatch of streaming data.
# MAGIC 
# MAGIC The Databricks Runtime provides guarantees that these <a href="https://docs.databricks.com/delta/delta-streaming.html#idempot-write" target="_blank">streaming Delta Lake writes will be idempotent</a>, even when writing to multiple tables, IF you set the "txnVersion" and "txnAppId" options. This is especially useful when data for multiple tables might be contained within a single record.  This was added in <a href="https://docs.databricks.com/release-notes/runtime/8.4.html" target="_blank">Databricks Runtime 8.4</a>.
# MAGIC 
# MAGIC The code below first defines the custom writer logic to append records to two new tables, and then demonstrates using this function within **`foreachBatch`**.
# MAGIC 
# MAGIC There is some debate as to whether you should use foreachBatch to write to multiple tables or to simply use multiple streams.  Generally multiple streams is the simpler and more efficient design because it allows streaming jobs writing to each table to run independently of each other.  Whereas using foreachBatch to write to multiple tables has the advantage of keeping writes to the two tables in sync.

# COMMAND ----------

def write_twice(microBatchDF, batchId):
    appId = "write_twice"
    
    microBatchDF.select("insert comma separated column names here", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("bronze_table_1")
    
    microBatchDF.select("insert comma separated column names here", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("bronze_table_2")


def autoload_to_multiple_tables(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .foreachBatch(write_twice)
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .trigger(availableNow=True)
                  .table(table_name))
    
    query.awaitTermination()
