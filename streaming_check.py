# Databricks notebook source
IOT_CS = "Endpoint=sb://ihsuprodpnres019dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=FS7yH98azfRbvfhHW/q+nrsTTTSz/42pnescbtJoVFU=;EntityPath=iothub-ehub-iothubchec-24758816-65b2d26c78" # dbutils.secrets.get('iot','iothub-cs') # IoT Hub connection string (Event Hub Compatible)
ehConf = { 
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(IOT_CS),
  'ehName':"iothub-ehub-iothubchec-24758816-65b2d26c78"
}


# COMMAND ----------


spark.conf.set("fs.azure.account.key.zee.dfs.core.windows.net", "YlBjJI7gIJpTUQ5ELXbqiqI/T/WhLRDi03zTBEiVU30O0O9zXwkWqKULPst5k+5cors7NYTbyw/v+AStJ3bIBw==")

# Setup storage locations for all data
ROOT_PATH = f"abfss://iot@zee.dfs.core.windows.net/"
BRONZE_PATH = ROOT_PATH + "bronze/"
SILVER_PATH = ROOT_PATH + "silver/"
GOLD_PATH = ROOT_PATH + "gold/"
SYNAPSE_PATH = ROOT_PATH + "synapse/"
CHECKPOINT_PATH = ROOT_PATH + "checkpoints/"

# COMMAND ----------

from pyspark.sql import functions as F

from pyspark.sql.functions import *


# Schema of incoming data from IoT hub
schema = "timestamp timestamp, deviceId string, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"


# Read directly from IoT Hubs using the EventHubs library for Azure Databricks
iot_stream = (
    spark.readStream.format("eventhubs")                                        # Read from IoT Hubs directly
    .options(**ehConf)                                                        # Use the Event-Hub-enabled connect string
    .load()                                                                   # Load the data
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema)) # Extract the payload from the messages
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))        # Create a "date" field for partitioning
)

# Split our IoT Hubs stream into separate streams and write them both into their own Delta locations
write_turbine_to_delta = (
    iot_stream.filter('temperature is null')                          # Filter out turbine telemetry from other streams
    .select('date','timestamp','deviceId','rpm','angle')            # Extract the fields of interest
    .writeStream.format('delta')                                    # Write our stream to the Delta format
    .partitionBy('date')                                            # Partition our data by Date for performance
    .option("checkpointLocation", ROOT_PATH + "/bronze/cp/turbine") # Checkpoint so we can restart streams gracefully
    .start(ROOT_PATH + "/bronze/data/turbine_raw")                  # Stream the data into an ADLS Path
)

# COMMAND ----------



# COMMAND ----------

display(iot_stream)

# COMMAND ----------



# COMMAND ----------

# Create functions to merge turbine and weather data into their target Delta tables
def merge_records(incremental, target_path): 
    incremental.createOrReplaceTempView("incremental")
    
# MERGE consists of a target table, a source table (incremental),
# a join key to identify matches (deviceid, time_interval), and operations to perform 
# (UPDATE, INSERT, DELETE) when a match occurs or not
    incremental._jdf.sparkSession().sql(f"""
        MERGE INTO turbine_hourly t
        USING incremental i
        ON i.date=t.date AND i.deviceId = t.deviceid AND i.time_interval = t.time_interval
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# Perform the streaming merge into our  data stream
turbine_stream = (
    spark.readStream.format('delta').table('turbine_raw')        # Read data as a stream from our source Delta table
    .groupBy('deviceId','date',F.window('timestamp','1 hour')) # Aggregate readings to hourly intervals
    .agg({"rpm":"avg","angle":"avg"})
    .writeStream                                                                                         
    .foreachBatch(merge_records)                              # Pass each micro-batch to a function
    .outputMode("update")                                     # Merge works with update mod
    .start()
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

iot_stream = (
    spark.readStream.format("eventhubs")                                        # Read from IoT Hubs directly
    .options(**ehConf)                                                        # Use the Event-Hub-enabled connect string
    .load()                                                                   # Load the data
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema)) # Extract the payload from the messages
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))        # Create a "date" field for partitioning
)


(iot_stream.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", "/tmp/delta/_checkpoints/")
   .start("/delta/events")
)


# COMMAND ----------

IOT_CS1 = "Endpoint=sb://iothub-ns-iotsource-24760747-aece8adb55.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=avxtSdCezB2uw9o4cFlkg7X3pBNDxV8YHTmdHLWF1OU=;EntityPath=iotsource" # dbutils.secrets.get('iot','iothub-cs') # IoT Hub connection string (Event Hub Compatible)
ehConf = { 
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(IOT_CS1),
  'ehName':"iotsource"
}


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# from delta.tables import *

# deltaTable = DeltaTable.forPath(spark, "/data/mydata")



iot_stream1 = (
    spark.readStream.format("eventhubs")                                        # Read from IoT Hubs directly
    .options(**ehConf)                                                        # Use the Event-Hub-enabled connect string
    .load()                                                                   # Load the data
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema)) # Extract the payload from the messages
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))        # Create a "date" field for partitioning
)



(iot_stream1.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", "dbfs:/user/hive/warehouse/zee/")
   .toTable("events1")
)




# COMMAND ----------

display(iot_stream)
