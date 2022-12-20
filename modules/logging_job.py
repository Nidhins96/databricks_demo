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

from azure.cosmos import errors
from azure.cosmos import documents
from azure.cosmos import http_constants
from azure.cosmos import exceptions, CosmosClient, PartitionKey


from storage_service import storage_connect
from deltatable_service import table_read_write

# COMMAND ----------

# DBTITLE 1,Read Configuration Details
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

# DBTITLE 1,Read the current time
def curr_date():
    # Importing the datetime module
    import datetime

    # Storing the current date and time in
    # a new variable using the datetime.now()
    # function of datetime module
    current_date = datetime.datetime.now()

    # Replacing the value of the timezone in tzinfo class of
    # the object using the replace() function
    current_date = current_date.\
        replace(tzinfo=datetime.timezone.utc)

    # Converting the date value into ISO 8601
    # format using isoformat() method
    current_date = current_date.isoformat()
    return current_date

# COMMAND ----------

# DBTITLE 1,EventHub to Delta Table
spark_log = spark._jvm.org.apache.log4j
logger = spark_log.LogManager.getLogger(__name__)

from read_storeconnect_data import read_storeconnect_data
new = read_storeconnect_data(spark , logger)
new.run_workflow()

# COMMAND ----------

# DBTITLE 1,Define Schemas for different Origin Types


# COMMAND ----------

# DBTITLE 1,Read Streaming data from Delta Table
from pyspark.sql.streaming import StreamingQueryListener

# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryManager

# COMMAND ----------

# DBTITLE 1,Read Historical Data from Delta Table


# COMMAND ----------

# DBTITLE 1,Setup Cosmos DB


# COMMAND ----------

# DBTITLE 1,Write Historical Data to Cosmos DB


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Write Streaming Data to Cosmos DB
spark_log = spark._jvm.org.apache.log4j
logger = spark_log.LogManager.getLogger(__name__)

logger.info("new info Logging")
logger.warn("warn Logging")
logger.error("error logging")


# COMMAND ----------


