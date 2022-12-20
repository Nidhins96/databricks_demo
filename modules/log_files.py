# Databricks notebook source
spark_log = spark._jvm.org.apache.log4j
logger = spark_log.LogManager.getLogger(__name__)

# COMMAND ----------

logger.info("info Logging")
logger.warn("warn Logging")
logger.error("error logging")


# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/new_demo")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


