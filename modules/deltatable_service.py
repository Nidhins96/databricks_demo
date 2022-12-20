from delta.tables import *
class table_read_write:
    
    def createtable(self ,MountFolder, spark):
        
        DeltaTable.createIfNotExists(spark) \
      .addColumn("_id", "STRING") \
      .addColumn("eventType", "STRING") \
      .addColumn("createdAt", "STRING") \
      .addColumn("status", "STRING") \
      .addColumn("priorStatus", "STRING") \
      .addColumn("site", "STRING") \
      .addColumn("origin_id", "STRING") \
      .addColumn("origin_type", "STRING") \
      .addColumn("eventNotification", "STRING") \
      .addColumn("elpUrl", "STRING") \
      .location("/mnt/{}".format(MountFolder)) \
      .execute()
    
    
    def write_to_table (self,MountFolder,df):
        
        '''Data with delta table schema is taken as input and written to location as delta table'''
		
        saveloc = "/mnt/{}".format(MountFolder)
        streamQ = (df.writeStream.format("delta").outputMode("append")
           .option("checkpointLocation",f"{saveloc}/_checkpoint")
           .start(saveloc))
        
        
    def read_from_table(self,MountFolder,spark):
        
        '''Data with delta table is read which can then be used for further processing'''
		
        saveloc = "/mnt/{}".format(MountFolder)
        
        # df = spark.readStream.format("delta")\
        # .option("readChangeFeed", "true")\
        # .option("startingVersion", 0)\
        # .load(saveloc)
        df = (spark.readStream.format('delta').load(saveloc))
        return df