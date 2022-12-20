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

#dbutils.fs.unmount("/mnt/new_demo")

# COMMAND ----------

#display(dbutils.fs.mounts())

# COMMAND ----------

spark_log = spark._jvm.org.apache.log4j
logger = spark_log.LogManager.getLogger(__name__)


# COMMAND ----------

from read_storeconnect_data import read_storeconnect_data
new = read_storeconnect_data(spark,logger)
new.run_workflow()

# COMMAND ----------


