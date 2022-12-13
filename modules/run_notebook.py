# Databricks notebook source
# Import PySpark
import pyspark
#from pyspark.sql import SparkSession
from pyspark.sql.types import *
import  pyspark.sql.functions as F
from delta.tables import *
from pyspark.sql.functions import col
from storage import Storage
from schema import CustomSchema

# COMMAND ----------

import configparser

config = configparser.ConfigParser()
config.read(r'../config/config.ini')
connectionString = config.get('secrets',  'connection-string')
storageAccountName =  config.get('secrets',  'storageAccountName')
storageAccountAccessKey =  config.get('secrets',  'storageAccountAccessKey')
blobContainerName =  config.get('secrets',  'blobContainerName')
MountFolder = config.get('secrets',  'MountFolder')

# COMMAND ----------

saveloc = "/mnt/{}".format(MountFolder)

# COMMAND ----------

from Eventhub_connector import EventhubConnect

eh_connect = EventhubConnect(spark)

# COMMAND ----------

ehConf = eh_connect.config_return(connectionString)

# COMMAND ----------

s = Storage(spark)

# COMMAND ----------

s.Sub_mount(blobContainerName,storageAccountName,storageAccountAccessKey,MountFolder)

# COMMAND ----------

#s.Sub_unmount(MountFolder)

# COMMAND ----------


#cs = CustomSchema()
#structureSchema = cs.Return_Schema()
df = spark.readStream.format("eventhubs").options(**ehConf).load()
decoded_df = df.select(col("body").cast("STRING"))

# COMMAND ----------

display(df)

# COMMAND ----------

from converttodeltaformat import FormatDelta
fd = FormatDelta()
new_df= fd.ConverttoDelta(df)

# COMMAND ----------

display(new_df)

# COMMAND ----------

# saveloc = "/mnt/{}".format(MountFolder)

# streamQ = (decoded_df_new.writeStream.format("delta")
#            .option("checkpointLocation",f"{saveloc}/_checkpoint")
#            .start(saveloc))

# COMMAND ----------

from writetodelta import SaveTable 

# COMMAND ----------


st = SaveTable()
st.write_to_table(MountFolder,new_df)

# COMMAND ----------

saveloc = "/mnt/{}".format(MountFolder)
saveloc

# COMMAND ----------



# COMMAND ----------

ehConf = eh_connect.config_return(connectionString)

# COMMAND ----------

ehConf

# COMMAND ----------


#cs = CustomSchema()
#structureSchema = cs.Return_Schema()
df = spark.readStream.format("eventhubs").options(**ehConf).load()
decoded_df = df.select(col("body").cast("STRING"))

# COMMAND ----------

display(decoded_df)

# COMMAND ----------


