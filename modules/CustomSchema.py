from pyspark.sql.types import *
class CustomSchema:
    '''custom schema defined '''
	
    structureSchema = StructType([StructField('_id', StringType(), True), StructField('createdAt', StringType(), True), StructField('elpUrl', StringType(), True),       StructField('eventType', StringType(), True), StructField('origin', StructType([StructField('data', StructType([StructField('Address', StringType(), True), StructField('City', StringType(), True), StructField('CreateDateTime', StringType(), True), StructField('DeleteDateTime', StringType(), True), StructField('Description', StringType(), True), StructField('DiscoveryName', StringType(), True), StructField('ID', StringType(), True), StructField('InitialInserviceTimestamp', StringType(), True), StructField('IsInService', StringType(), True), StructField('IsVirtual', StringType(), True), StructField('Name', StringType(), True), StructField('OrganizationName', StringType(), True), StructField('State', StringType(), True), StructField('Status', StringType(), True), StructField('Test', StringType(), True), StructField('Timezone', StringType(), True), StructField('UXName', StringType(), True), StructField('Zip', StringType(), True)]), True), StructField('id', StringType(), True), StructField('type', StringType(), True)]), True), StructField('priorStatus', StringType(), True), StructField('site', StringType(), True), StructField('status', StringType(), True)])
    
    
    def Return_Schema(self):
        '''The custom schema defined is returned for processing '''
		
        return self.structureSchema