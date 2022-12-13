from py4j.java_gateway import java_import


class EventhubConnect:
    
    def __init__(self,spark):
        # Initialization of the Strings
        self.spark = spark 
        java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
        
    
    def config_return(self,connectionString):
        ehConf = {}
        ehConf['eventhubs.connectionString'] = connectionString

        # Add consumer group to the ehConf dictionary

        ehConf['eventhubs.consumerGroup'] = "$Default"

        # Encrypt ehConf connectionString property

        ehConf['eventhubs.connectionString'] = self.spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
        
        return ehConf