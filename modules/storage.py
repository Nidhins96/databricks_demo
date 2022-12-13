
class Storage: 
    def __init__(self,spark):
        # Initialization of the Strings
        self.spark = spark 
    def Sub_mount(self,blobContainerName,storageAccountName,storageAccountAccessKey,MountFolder):
        """Define storage account and container name from config file 
           Define mount point location from azure stoarge account
           Define storage access key from config file
        """
        dbutils = self.Get_dbutils( self.spark)
        try:
            dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
            mount_point = "/mnt/{}".format(MountFolder),
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey})
            print("mounted.")
        except :
            print("already mounted. Try to unmount first")

    #Function to umnount container from stoarge account
    def Sub_unmount(self,MountFolder):
        dbutils = self.Get_dbutils( self.spark)
        str_path = "/mnt/{}".format(MountFolder)
        if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(str_path)
            print("Unmounted")
        else:
            print("file not mounted")
    def Get_dbutils(self,spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils
# dbutils = get_dbutils(spark)