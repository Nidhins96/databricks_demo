from schema import CustomSchema
import  pyspark.sql.functions as F
class FormatDelta:
     
    def get_scehma(self):
        cs = CustomSchema()
        structureSchema = cs.Return_Schema()
        return structureSchema
    
    def ConverttoDelta(self,df):
        structureSchema = self.get_scehma()
        processed_df = df.select(F.from_json(F.col("body").cast("string"), structureSchema).getItem("_id").alias("_id"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("eventType").alias("eventType"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("createdAt").alias("createdAt"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("status").alias("status"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("priorStatus").alias("priorStatus"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("site").alias("site"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("origin").getItem("id").alias("origin_id"),\
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("origin").getItem("type").alias("origin_type"),\
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("origin").getItem("data").alias("data"), \
         F.from_json(F.col("body").cast("string"), structureSchema).getItem("elpUrl").alias("elpUrl"))
        
        return processed_df
    