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


mount = True

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

class read_storeconnect_data:
    
    def __init__(self,spark,logger):
        #Pass connection string to connect event hub and databricks
        self.spark = spark 
        self.logger = logger
        try:
            self.logger.info("Eventhub Connection spark session established")
            self.eh_connect = EventhubConnect(spark)
        except:
            self.logger.error("Spark connection not established to eventhub")

       
  
    def run_workflow(self):
        try:
            self.logger.info("Eventhub string Connection established")
            ehConf = self.eh_connect.config_return(connectionString)
        except:
            self.logger.error("Eventhub Connection string not established. Check connection string config file")
        
        try:
            self.logger.info("storage Account  spark session connection established")
            s = storage_connect(self.spark)
        except:
            self.logger.error("Spark connection not established to storage Account")
       
         #mount container from stoarge account
        try:
            s.sub_mount(blobContainerName,storageAccountName,storageAccountAccessKey,MountFolderpath)
        except:
            s.sub_unmount(MountFolderpath)

        #Load streaming data from eventhub as pyspark dataframe     
        try:
            
           
            df = self.spark.readStream.format("eventhubs").options(**ehConf).load()
            self.logger.info("Event data successfuly streamed from eventhub ")
        except Exception as e:
            self.logger.error("event data not streamed from event hub. Error {}".format(e.__class__))
            self.logger.warn("Check connection string in config file path") 

        #The data is processed and initial streamed data is converted to schema for delta table
        fd = FormatDelta()
        try:
            new_df= fd.ConverttoDelta(df)
            self.logger.info("Event logged ")
        except:
            self.logger.error("Event data processing failed.")
        
        try:
            st = table_read_write()
            st.write_to_table(MountFolderpath,new_df)
            self.logger.info("Event written to delta table ")
        except:
            self.logger.error("Event not written to delta table ")
        #The delta tabe schema data is saved to delta table
        
    
