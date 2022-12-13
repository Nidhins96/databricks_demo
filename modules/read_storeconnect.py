# Databricks notebook source
import configparser

config = configparser.ConfigParser()
config.read(r'../config/config.ini')

# COMMAND ----------

connectionString = config.get('secrets',  'connection-string')
storageAccountName =  config.get('secrets',  'storageAccountName')
storageAccountAccessKey =  config.get('secrets',  'storageAccountAccessKey')
blobContainerName =  config.get('secrets',  'blobContainerName')
MountFolder = config.get('secrets',  'MountFolder')

# COMMAND ----------

unmount = False
mount = True

# COMMAND ----------

from py4j.java_gateway import java_import
java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")

ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString

# Add consumer group to the ehConf dictionary

ehConf['eventhubs.consumerGroup'] = "$Default"

# Encrypt ehConf connectionString property

ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# COMMAND ----------

def sub_mount(str_path):
    if any(mount.mountPoint != str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
        mount_point = "/mnt/{}".format(MountFolder),
        extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey})
        
def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)
    else:
        print("file not mounted")
        

# COMMAND ----------


try:
    sub_mount("/mnt/{}".format(MountFolder))
    print("Container Mounted")
except :
    print("already mounted. Try to unmount first")

# COMMAND ----------

unmount = False
if unmount == True:
    try:
        sub_unmount("/mnt/{}".format(MountFolder))
    except:
        print("File not Mounted")

# COMMAND ----------

if mount == True:
    try:
        sub_mount("/mnt/{}".format(MountFolder))
        print("Container Mounted")
    except :
        print("already mounted. Try to unmount first")

# COMMAND ----------

sub_unmount("/mnt/{}".format(MountFolder))

# COMMAND ----------



# COMMAND ----------

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils

dbutils = get_dbutils(spark)

# COMMAND ----------

import configparser
from py4j.java_gateway import java_import
java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")

#Read config file from location
config = configparser.ConfigParser()
config.read(r'../config/config.ini')

#Reading secrets from config file

connectionString = config.get('secrets',  'connection-string')
storageAccountName =  config.get('secrets',  'storageAccountName')
storageAccountAccessKey =  config.get('secrets',  'storageAccountAccessKey')
blobContainerName =  config.get('secrets',  'blobContainerName')
MountFolder = config.get('secrets',  'MountFolder')

# COMMAND ----------

# Import PySpark
import pyspark
from pyspark.sql import SparkSession

#Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

# COMMAND ----------

from storage import Storage
s = Storage(spark)
s.Sub_mount("/mnt/{}",blobContainerName,storageAccountName,storageAccountAccessKey,MountFolder)

# COMMAND ----------

dbutils

# COMMAND ----------

from read_storeconnect_event import sub_mount

# COMMAND ----------

MountFolder = config.get('schema',  'customschema')
MountFolder

# COMMAND ----------


