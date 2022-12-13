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


# Mount parameter set to True as default 

mount = True
unmount = False


#Creating connection string configuration for connection between databricks and event hub
ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString

# Add consumer group to the ehConf dictionary

ehConf['eventhubs.consumerGroup'] = "$Default"

# Encrypt ehConf connectionString property

ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)


#Function to mount container from stoarge account
def sub_mount(str_path):
    """Define storage account and container name from config file 
       Define mount point location from azure stoarge account
       Define storage access key from config file
    """
    if any(mount.mountPoint != str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
        mount_point = "/mnt/{}".format(MountFolder),
        extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey})
        
#Function to umnount container from stoarge account
def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)
    else:
        print("file not mounted")
        
if mount == True:
    try:
        sub_mount("/mnt/{}".format(MountFolder))
        print("Container Mounted")
    except :
        print("already mounted. Try to unmount first")
        
        
unmount = False
if unmount == True:
    try:
        sub_unmount("/mnt/{}".format(MountFolder))
    except:
        print("File not Mounted")
        
        
        
 
        
        