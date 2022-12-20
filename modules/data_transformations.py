from CustomSchema import CustomSchema
import  pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, IntegerType, ArrayType, StringType, DateType, MapType, StructField

class FormatDelta:
    
    def get_scehma(self):
        '''Input Custom schema created in file and pass the schema for processing'''
        cs = CustomSchema()
        structureSchema = cs.Return_Schema()
        return structureSchema
    
    def ConverttoDelta(self,df):
        '''Input data from eventhub is converted to schema specified for delta table'''
        structureSchema = self.get_scehma()
        processed_df = df.select(F.from_json(F.col("body").cast("string"), structureSchema).getItem("_id").alias("_id"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("eventType").alias("eventType"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("createdAt").alias("createdAt"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("status").alias("status"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("priorStatus").alias("priorStatus"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("site").alias("site"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("origin").getItem("id").alias("origin_id"),\
         F.from_json(F.col("body").cast("string"), structureSchema)\
                                 .getItem("origin").getItem("type").alias("origin_type"),\
                                 col("body").cast("STRING").alias("eventNotification"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("elpUrl").alias("elpUrl"))
        
        return processed_df
    