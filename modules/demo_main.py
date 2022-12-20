# Databricks notebook source
#Read a delta table from location once
new_df = (spark
         .read
         .format('delta')
         .load("/mnt/new_folder_demo")) 
display(new_df)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#Read a delta table from location once
new_df = (spark
         .readStream
         .format('delta')
         .load("/mnt/simulator_data")) 
display(new_df)

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
from transformations import transformations
from cosmosdb_connect import cosmosdb_connect
from cosmosdb_setup import create_cosmos_db_container
from cosmosdb_service import cosmosdb_read_write

# COMMAND ----------

#Read config file from location
config = configparser.ConfigParser()
config.read(r'../config/config1.ini')

#Reading secrets from config file

connectionString = config.get('secrets',  'connection-string')
storageAccountName =  config.get('secrets',  'storageAccountName')
storageAccountAccessKey =  config.get('secrets',  'storageAccountAccessKey')
blobContainerName =  config.get('secrets',  'blobContainerName')
MountFolderpath = config.get('secrets',  'MountFolderpath')
consumerGroup= config.get('secrets',  'consumerGroup')


cosmosEndpoint = config.get('secrets_CosmosDB', 'cosmosEndpoint')
cosmosMasterKey = config.get('secrets_CosmosDB', 'cosmosMasterKey')
cosmosDatabaseName = config.get('secrets_CosmosDB', 'cosmosDatabaseName')
cosmosContainerName = config.get('secrets_CosmosDB', 'cosmosContainerName')
cosmosPartitionKey = config.get('secrets_CosmosDB', 'PartitionKeyPath')

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
}

# COMMAND ----------

print(blobContainerName)

# COMMAND ----------

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

ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString

# Add consumer group to the ehConf dictionary

ehConf['eventhubs.consumerGroup'] = "$Default"

# Encrypt ehConf connectionString property

ehConf['eventhubs.connectionString'] = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
df = spark.readStream.format("eventhubs").options(**ehConf).load()

# COMMAND ----------

display(df)

# COMMAND ----------

fd = FormatDelta()
new_df= fd.ConverttoDelta(df)

# COMMAND ----------

display(new_df)

# COMMAND ----------

st = table_read_write()
st.write_to_table(MountFolderpath,new_df)

# COMMAND ----------

def innerSchema(event_type):
    if event_type == 'hub':
        schema = StructType([
                          StructField("Name", StringType(), True),
                          StructField("OrganizationName", StringType(), True),
                          StructField("Test", StringType(), True),
                          StructField("IsVirtual", StringType(), True)])
    elif event_type == 'site':
        schema = StructType([
                          StructField("UXName", StringType(), True),
                          StructField("Description", StringType(), True),
                          StructField("CreateDateTime", StringType(), True),
                          StructField("DeleteDateTime", StringType(), True),
                          StructField("Status", StringType(), True)])
    elif event_type == 'device':
        schema = StructType([
                          StructField("Name", StringType(), True),
                          StructField("Description", StringType(), True),
                          StructField("Address", StringType(), True),
                          StructField("City", StringType(), True),
                          StructField("State", StringType(), True),
                          StructField("Zip", StringType(), True),
                          StructField("Timezone", StringType(), True),
                          StructField("Test", StringType(), True),
                          StructField("IsInService", StringType(), True),
                          StructField("InitialInserviceTimestamp", StringType(), True),
                          StructField("OrganizationName", StringType(), True)])
    return schema
	
	
def OuterSchema(event_type):
    outerSchema = StructType([ 
        StructField("_id",StringType(),True), 
        StructField("createdAt",StringType(),True), 
        StructField("elpUrl",StringType(),True), 
        StructField("eventType", StringType(), True),
        StructField("site", StringType(), True),
        StructField('origin', StructType([
                    StructField("id", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("data",innerSchema(event_type), True)
        ])),
        StructField("status", StringType(), True),
        StructField("priorStatus", StringType(), True)])
    return outerSchema

# COMMAND ----------

readdl = table_read_write()
df = readdl.read_from_table(MountFolderpath, spark)
    
    
df = df.withColumn("workflowStatusId", lit(1))
df = df.withColumn("comments", lit(""))
df = df.withColumn("updatedDate", lit(""))
df = df.withColumn("receivedDate", F.lit(curr_date()))
df = df.withColumn("siteName", lit(""))
df = df.withColumn("eventName", lit(""))

# Data Transformations

df_device = df.where(df.origin_type == "device")
df_device = df_device.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('device')))
df_device = df_device.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

df_hub = df.where(df.origin_type == "hub")
df_hub = df_hub.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('hub')))
df_hub = df_hub.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

df_site = df.where(df.origin_type == "site")
df_site = df_site.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('site')))
df_site = df_site.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Read Historical Data from Delta Table
hdf = (spark.read.format('delta').load("/mnt/{}".format(MountFolderpath)))

hdf = hdf.withColumn("workflowStatusId", lit(1))
hdf = hdf.withColumn("comments", lit(""))
hdf = hdf.withColumn("updatedDate", lit(""))
hdf = hdf.withColumn("receivedDate", F.lit(curr_date()))
hdf = hdf.withColumn("siteName", lit(""))
hdf = hdf.withColumn("eventName", lit(""))

# Data Transformations

hdf_device = hdf.where(hdf.origin_type == "device")
hdf_device = hdf_device.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('device')))
hdf_device = hdf_device.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

hdf_hub = hdf.where(hdf.origin_type == "hub")
hdf_hub = hdf_hub.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('hub')))
hdf_hub = hdf_hub.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

hdf_site = hdf.where(hdf.origin_type == "site")
hdf_site = hdf_site.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('site')))
hdf_site = hdf_site.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')
hdf = hdf.withColumn("workflowStatusId", lit(1))
hdf = hdf.withColumn("comments", lit(""))
hdf = hdf.withColumn("updatedDate", lit(""))
hdf = hdf.withColumn("receivedDate", F.lit(curr_date()))

# Data Transformations

hdf_device = hdf.where(hdf.origin_type == "device")
hdf_device = hdf_device.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('device')))
hdf_device = hdf_device.select(col("_id").alias("id"), 'workflowStatusID', 'receivedDate', 'updatedDate', 'comments', 
                               'origin_type', 'eventNotification')

hdf_hub = hdf.where(hdf.origin_type == "hub")
hdf_hub = hdf_hub.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('hub')))
hdf_hub = hdf_hub.select(col("_id").alias("id"), 'workflowStatusID', 'receivedDate', 'updatedDate', 'comments', 
                         'origin_type', 'eventNotification')

hdf_site = hdf.where(hdf.origin_type == "site")
hdf_site = hdf_site.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('site')))
hdf_site = hdf_site.select(col("_id").alias("id"), 'workflowStatusID', 'receivedDate', 'updatedDate', 'comments', 
                           'origin_type', 'eventNotification')

# COMMAND ----------

# DBTITLE 1,Setup Cosmos DB
# Connect to Cosmos DB and create database and container

client = cosmosdb_connect(cosmosEndpoint, cosmosMasterKey).define_client()
cosmosdbcontainer = create_cosmos_db_container(cosmosDatabaseName, cosmosContainerName, cosmosPartitionKey, 400)
database = cosmosdbcontainer.create_cosmosdatabase(client)
container = cosmosdbcontainer.create_cosmoscontainer(database)

# COMMAND ----------

# DBTITLE 1,Write Historical Data to Cosmos DB
hdf_device.write.format("cosmos.oltp")\
        .options(**cfg)\
        .mode("APPEND")\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Device/")\
        .save()

hdf_hub.write.format("cosmos.oltp")\
        .options(**cfg)\
        .mode("APPEND")\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Hub/")\
        .save()

hdf_site.write.format("cosmos.oltp")\
        .options(**cfg)\
        .mode("APPEND")\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Site/")\
        .save()

# COMMAND ----------

# DBTITLE 1,Write Streaming Data to Cosmos DB
df_device.writeStream.format("cosmos.oltp")\
        .options(**cfg)\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Device/")\
        .start()

df_hub.writeStream.format("cosmos.oltp")\
        .options(**cfg)\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Hub/")\
        .start()

df_site.writeStream.format("cosmos.oltp")\
        .options(**cfg)\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Site/")\
        .start()

# COMMAND ----------

# Connect to Cosmos DB and create database and container

client = cosmosdb_connect(cosmosEndpoint, cosmosMasterKey).define_client()
cosmosdbcontainer = create_cosmos_db_container(cosmosDatabaseName, cosmosContainerName, cosmosPartitionKey, 400)
database = cosmosdbcontainer.create_cosmosdatabase(client)
container = cosmosdbcontainer.create_cosmoscontainer(database)

# COMMAND ----------

df_device.writeStream.format("cosmos.oltp")\
        .options(**cfg)\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/device/")\
        .start()

df_hub.writeStream.format("cosmos.oltp")\
        .options(**cfg)\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/hub/")\
        .start()

df_site.writeStream.format("cosmos.oltp")\
        .options(**cfg)\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/site/")\
        .start()

# COMMAND ----------

# def writeStreamer(input): 
    
#     StreamingQuery = input.writeStream.format("cosmos.oltp")\
#         .options(**cfg)\
#         .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
#         .option("checkpointLocation", "/tmp/myRunId/")\
#         .outputMode("Append")\
#         .start()
#     return StreamingQuery

# writeStreamer(df_device)
# writeStreamer(df_hub)
# writeStreamer(df_site)

# COMMAND ----------



# COMMAND ----------

dbutils.fs.mount(source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
            mount_point = "/mnt/{}".format(MountFolderpath),
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey})

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

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
from transformations import transformations
from cosmosdb_connect import cosmosdb_connect
from cosmosdb_setup import create_cosmos_db_container
from cosmosdb_service import cosmosdb_read_write

# COMMAND ----------

# DBTITLE 1,Read Configuration Details
#Read config file from location
config = configparser.ConfigParser()
config.read(r'../config/config1.ini')

#Reading secrets from config file

connectionString = config.get('secrets',  'connection-string')
storageAccountName =  config.get('secrets',  'storageAccountName')
storageAccountAccessKey =  config.get('secrets',  'storageAccountAccessKey')
blobContainerName =  config.get('secrets',  'blobContainerName')
MountFolderpath = config.get('secrets',  'MountFolderpath')
consumerGroup= config.get('secrets',  'consumerGroup')


cosmosEndpoint = config.get('secrets_CosmosDB', 'cosmosEndpoint')
cosmosMasterKey = config.get('secrets_CosmosDB', 'cosmosMasterKey')
cosmosDatabaseName = config.get('secrets_CosmosDB', 'cosmosDatabaseName')
cosmosContainerName = config.get('secrets_CosmosDB', 'cosmosContainerName')
cosmosPartitionKey = config.get('secrets_CosmosDB', 'PartitionKeyPath')

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
}

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
def innerSchema(event_type):
    
    if event_type == 'hub':
        schema = StructType([
                          StructField("Name", StringType(), True),
                          StructField("OrganizationName", StringType(), True),
                          StructField("Test", StringType(), True),
                          StructField("IsVirtual", StringType(), True)
                           ])
        
    elif event_type == 'site':
        schema = StructType([
                          StructField("ID", StringType(), True),
                          StructField("Name", StringType(), True),
                          StructField("Description", StringType(), True),
                          StructField("Address", StringType(), True),
                          StructField("City", StringType(), True),
                          StructField("State", StringType(), True),
                          StructField("Zip", StringType(), True),
                          StructField("Timezone", StringType(), True),
                          StructField("IsInService", IntegerType(), True),
                          StructField("InitialInserviceTimestamp", DateType(), True),
                          StructField("OrganizationName", StringType(), True)
                           ])
        
    elif event_type == 'device':
        schema = StructType([
                          StructField("DiscoveryName", StringType(), True),
                          StructField("UXName", StringType(), True),
                          StructField("Description", StringType(), True),
                          StructField("CreateDateTime", DateType(), True),
                          StructField("DeleteDateTime", DateType(), True),
                          StructField("Status", StringType(), True),
                           ])
    return schema
	
	
def OuterSchema(event_type):
    
    outerSchema = StructType([ 
        StructField("_id",StringType(),True), 
        StructField("createdAt",DateType(),True), 
        StructField("elpUrl",StringType(),True), 
        StructField("eventType", StringType(), True),
        StructField("site", StringType(), True),
        StructField('origin', StructType([
                    StructField("id", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("data",innerSchema(event_type), True)
        ])),
        StructField("status", StringType(), True),
        StructField("priorStatus", StringType(), True)])
    return outerSchema

# COMMAND ----------

# DBTITLE 1,Read Streaming data from Delta Table
readdl = table_read_write()
df = readdl.read_from_table(MountFolderpath, spark)
    
    
df = df.withColumn("workflowStatusId", lit(1))
df = df.withColumn("comments", lit(""))
df = df.withColumn("updatedDate", lit(""))
df = df.withColumn("receivedDate", F.lit(curr_date()))
df = df.withColumn("siteName", lit(""))
df = df.withColumn("eventName", lit(""))

# Data Transformations

df_device = df.where(df.origin_type == "device")
df_device = df_device.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('device')))
df_device = df_device.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

df_hub = df.where(df.origin_type == "hub")
df_hub = df_hub.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('hub')))
df_hub = df_hub.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

df_site = df.where(df.origin_type == "site")
df_site = df_site.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('site')))
df_site = df_site.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Read Historical Data from Delta Table
hdf = (spark.read.format('delta').load("/mnt/{}".format(MountFolderpath)))

hdf = hdf.withColumn("workflowStatusId", lit(1))
hdf = hdf.withColumn("comments", lit(""))
hdf = hdf.withColumn("updatedDate", lit(""))
hdf = hdf.withColumn("receivedDate", F.lit(curr_date()))
hdf = hdf.withColumn("siteName", lit(""))
hdf = hdf.withColumn("eventName", lit(""))

# Data Transformations

hdf_device = hdf.where(hdf.origin_type == "device")
hdf_device = hdf_device.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('device')))
hdf_device = hdf_device.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

hdf_hub = hdf.where(hdf.origin_type == "hub")
hdf_hub = hdf_hub.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('hub')))
hdf_hub = hdf_hub.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')

hdf_site = hdf.where(hdf.origin_type == "site")
hdf_site = hdf_site.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('site')))
hdf_site = hdf_site.select(col("_id").alias("id"), 'workflowStatusID', 'createdAt', 'receivedDate', 'updatedDate', 'comments',
                           'origin_type', 'eventType', 'eventName', 'site', 'siteName', 'eventNotification')
hdf = hdf.withColumn("workflowStatusId", lit(1))
hdf = hdf.withColumn("comments", lit(""))
hdf = hdf.withColumn("updatedDate", lit(""))
hdf = hdf.withColumn("receivedDate", F.lit(curr_date()))

# Data Transformations

hdf_device = hdf.where(hdf.origin_type == "device")
hdf_device = hdf_device.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('device')))
hdf_device = hdf_device.select(col("_id").alias("id"), 'workflowStatusID', 'receivedDate', 'updatedDate', 'comments', 
                               'origin_type', 'eventNotification')

hdf_hub = hdf.where(hdf.origin_type == "hub")
hdf_hub = hdf_hub.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('hub')))
hdf_hub = hdf_hub.select(col("_id").alias("id"), 'workflowStatusID', 'receivedDate', 'updatedDate', 'comments', 
                         'origin_type', 'eventNotification')

hdf_site = hdf.where(hdf.origin_type == "site")
hdf_site = hdf_site.withColumn("eventNotification", from_json(col('eventNotification'), OuterSchema('site')))
hdf_site = hdf_site.select(col("_id").alias("id"), 'workflowStatusID', 'receivedDate', 'updatedDate', 'comments', 
                           'origin_type', 'eventNotification')

# COMMAND ----------

# DBTITLE 1,Setup Cosmos DB
# Connect to Cosmos DB and create database and container

client = cosmosdb_connect(cosmosEndpoint, cosmosMasterKey).define_client()
cosmosdbcontainer = create_cosmos_db_container(cosmosDatabaseName, cosmosContainerName, cosmosPartitionKey, 400)
database = cosmosdbcontainer.create_cosmosdatabase(client)
container = cosmosdbcontainer.create_cosmoscontainer(database)

# COMMAND ----------

# DBTITLE 1,Write Historical Data to Cosmos DB
hdf_device.write.format("cosmos.oltp")\
        .options(**cfg)\
        .mode("APPEND")\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Device/")\
        .save()

hdf_hub.write.format("cosmos.oltp")\
        .options(**cfg)\
        .mode("APPEND")\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Hub/")\
        .save()

hdf_site.write.format("cosmos.oltp")\
        .options(**cfg)\
        .mode("APPEND")\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Site/")\
        .save()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Write Streaming Data to Cosmos DB

df_device.writeStream.format("cosmos.oltp")\
        .options(**cfg)\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Device/")\
        .start()

df_hub.writeStream.format("cosmos.oltp")\
        .options(**cfg)\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Hub/")\
        .start()

df_site.writeStream.format("cosmos.oltp")\
        .options(**cfg)\
        .option("spark.cosmos.changeFeed.startFrom", "Beginning")\
        .option("checkpointLocation", "/tmp/Site/")\
        .start()

# COMMAND ----------

display(df_device)

# COMMAND ----------

display(df_hub)

# COMMAND ----------


