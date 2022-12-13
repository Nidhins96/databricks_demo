class SaveTable:
    
    
    def write_to_table (self,MountFolder,df):
        saveloc = "/mnt/{}".format(MountFolder)
        streamQ = (df.writeStream.format("delta")
           .option("checkpointLocation",f"{saveloc}/_checkpoint")
           .start(saveloc))