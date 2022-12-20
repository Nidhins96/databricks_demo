# Databricks notebook source
import pyspark
from py4j.java_gateway import java_import
java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
from pyspark.sql.types import *
import  pyspark.sql.functions as F
from delta.tables import *
from pyspark.sql.functions import col, from_json, lit, create_map
# import org.apache.spark.sql.functions.{from_json,col}
from pyspark.sql.types import StructType, IntegerType, ArrayType, StringType, DateType, StructField

import pandas as pd
import configparser
import os
import json

# from azure.cosmos import errors
# from azure.cosmos import documents
# from azure.cosmos import http_constants
# from azure.cosmos import exceptions, CosmosClient, PartitionKey


from storage_service import storage_connect
from deltatable_service import table_read_write

# COMMAND ----------

# Import Packages
# Import Packages
import pyspark
from py4j.java_gateway import java_import
#java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
from pyspark.sql.types import *
import  pyspark.sql.functions as F
from delta.tables import *
from pyspark.sql.functions import col
from storage_service import storage_connect
from CustomSchema import CustomSchema
from deltatable_service import table_read_write 
from data_transformations import FormatDelta
import configparser
from Eventhub_connector import EventhubConnect
import pyspark
from py4j.java_gateway import java_import
java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
from pyspark.sql.types import *
import  pyspark.sql.functions as F
from delta.tables import *
from pyspark.sql.functions import col, from_json, lit, create_map
# import org.apache.spark.sql.functions.{from_json,col}
from pyspark.sql.types import StructType, IntegerType, ArrayType, StringType, DateType, StructField

# COMMAND ----------

# from pyspark.sql.
# import  pyspark.sql.functions as F
from pyspark.sql.streaming.listener import StreamingQueryListener

# COMMAND ----------

#Read config file from location
config = configparser.ConfigParser()
config.read(r'../config/config.ini')

#Reading secrets from config file

connectionString = config.get('secrets',  'connection-string')
storageAccountName =  config.get('secrets',  'storageAccountName')
storageAccountAccessKey =  config.get('secrets',  'storageAccountAccessKey')
blobContainerName =  config.get('secrets',  'blobContainerName')
MountFolderpath = config.get('secrets',  'MountFolderpath')
consumerGroup= config.get('secrets',  'consumerGroup')



# COMMAND ----------

# Observe metric
# observed_df = df.observe("metric", count(lit(1)).as("cnt"), count(col("error")).as("malformed"))
# observed_df.writeStream.format("...").start()

# Define my listener.
class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"'{event.name}' [{event.id}] got started!")
    def onQueryProgress(self, event):
        row = event.progress.observedMetrics.get("metric")
        if row is not None:
            if row.malformed / row.cnt > 0.5:
                print("ALERT! Ouch! there are too many malformed "
                      f"records {row.malformed} out of {row.cnt}!")
            else:
                print(f"{row.cnt} rows processed!")
    def onQueryTerminated(self, event):
        print(f"{event.id} got terminated!")

# Add my listener.
spark.streams.addListener(MyListener())

# COMMAND ----------

ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString

# Add consumer group to the ehConf dictionary

ehConf['eventhubs.consumerGroup'] = "$Default"

# Encrypt ehConf connectionString property

ehConf['eventhubs.connectionString'] = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# COMMAND ----------


df = spark.readStream.format("eventhubs").options(**ehConf).load()

# COMMAND ----------

display(df)

# COMMAND ----------

fd = FormatDelta()
new_df= fd.ConverttoDelta(df)
display(new_df)

# COMMAND ----------

df.writeStream.queryName('stream 11').format("console").start()

# COMMAND ----------

# Observe metric
# observed_df = df.observe("metric", count(lit(1)).as("cnt"), count(col("error")).as("malformed"))
# observed_df.writeStream.format("...").start()

# Define my listener.
class customMyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"'{event._id}' [{event.eventType}] got started!")
    def onQueryProgress(self, event):
        row = event.progress.observedMetrics.get("metric")
        if row is not None:
            if row.malformed / row.cnt > 0.5:
                print("ALERT! Ouch! there are too many malformed "
                      f"records {row.malformed} out of {row.cnt}!")
            else:
                print(f"{row.cnt} rows processed!")
    def onQueryTerminated(self, event):
        print(f"{event._id} got terminated!")


spark.streams.addListener(customMyListener())

# COMMAND ----------

new_df.writeStream.queryName('stream 12').format("console").start()

# COMMAND ----------



# COMMAND ----------

class TestListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        
        observed_metrics = event.progress.observedMetrics

    def onQueryTerminated(self, event):
        pass

spark.streams.addListener(TestListener())

df = spark.readStream.format("eventhubs").options(**ehConf).load()
#df = df.observe("metric", count(lit(1)).alias("cnt"), sum(col("value")).alias("sum"))
q = df.writeStream.format("noop").queryName("test").start()

# COMMAND ----------

class TestListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        row = event.progress.observedMetrics.get("metric")
        
        observed_metrics = event.progress.observedMetrics

    def onQueryTerminated(self, event):
        pass

spark.streams.addListener(TestListener())

#df = spark.readStream.format("eventhubs").options(**ehConf).load()
#df = df.observe("metric", count(lit(1)).alias("cnt"), sum(col("value")).alias("sum"))
q = new_df.writeStream.format("noop").queryName("test 1 ").start()


# COMMAND ----------

spark_log = spark._jvm.org.apache.log4j
logger = spark_log.LogManager.getLogger(__name__)


# COMMAND ----------

logger.info("my new info Logging")
logger.warn("warn Logging")
logger.error("error logging")


# COMMAND ----------

try:
    logger.info("Eventhub Connection spark session established")
    eh_connect = EventhubConnect(spark)
except:
    logger.error("Spark connection not established to eventhub")

       
  
   
try:
    logger.info("Eventhub string Connection established")
    ehConf =eh_connect.config_return(connectionString)
except:
    logger.error("Eventhub Connection string not established. Check connection string config file")



# COMMAND ----------

try:
    logger.info("storage Account spark session connection established")
    s = storage_connect(self.spark)
except:
    logger.error("Spark connection not established to storage Account")

# COMMAND ----------



# COMMAND ----------

# Define my listener.
class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"'{event.name}' [{event.id}] got started!")
        logger.info("new event ")
        logger.warn("new event warn")
        #logger.error("error logging")

    def onQueryProgress(self, event):
        row = event.progress.observedMetrics.get("metric")
        logger.info("new event in info Logging")
        logger.warn("new event in warn Logging")
        logger.error("new event in error logging")
        print(row)
        if row is not None:
            if row.malformed / row.cnt > 0.5:
                log.info("ALERT! Ouch! there are too many malformed "
                      f"records {row.malformed} out of {row.cnt}!")
                print("ALERT! Ouch! there are too many malformed "
                      f"records {row.malformed} out of {row.cnt}!")
            else:
                log.info(f"{row.cnt} rows processed!")
                print(f"{row.cnt} rows processed!")
    def onQueryTerminated(self, event):
        print(f"{event.id} got terminated!")


# Add my listener.
my_listener = MyListener()
spark.streams.addListener(my_listener)

df = spark.readStream.format("eventhubs").options(**ehConf).load()
#df = df.observe("metric", count(lit(1)).alias("cnt"), sum(col("value")).alias("sum"))
q = df.writeStream.format("console").queryName("new_test_latest").start()

# COMMAND ----------

# Define my listener.
class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"'{event.name}' [{event.id}] got started!")
    def onQueryProgress(self, event):
        row = event.progress.observedMetrics.get("metric")
        if row is not None:
            if row.malformed / row.cnt > 0.5:
                print("ALERT! Ouch! there are too many malformed "
                      f"records {row.malformed} out of {row.cnt}!")
            else:
                print(f"{row.cnt} rows processed!")
    def onQueryTerminated(self, event):
        print(f"{event.id} got terminated!")


# Add my listener.
my_listener = MyListener()
spark.streams.addListener(my_listener)

df = spark.readStream.format("eventhubs").options(**ehConf).load()
#df = df.observe("metric", count(lit(1)).alias("cnt"), sum(col("value")).alias("sum"))
q = df.writeStream.format("noop").queryName("new_test").start()

# COMMAND ----------


