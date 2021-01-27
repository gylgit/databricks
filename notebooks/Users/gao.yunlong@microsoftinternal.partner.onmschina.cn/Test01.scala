// Databricks notebook source
import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._

val eventHubName = "yleh"
val linkstr = "Endpoint=sb://yleventhub.servicebus.chinacloudapi.cn/;SharedAccessKeyName=PreviewDataPolicy;SharedAccessKey=TprGYZllOKxl00D3mhI7D4KodJBSBY3DN1q6nZVwxXk=;EntityPath=yleh"
val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder(linkstr).setEventHubName(eventHubName)
val customEventhubParameters = EventHubsConf(connStr.toString()).setMaxEventsPerTrigger(5)

// COMMAND ----------

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

incomingStream.printSchema

// Sending the incoming stream into the console.
// Data comes in batches!
incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Event Hub message format is JSON and contains "body" field
// Body is binary, so we cast it to string to see the actual content of the message
val messages = incomingStream.withColumn("Body", $"body".cast(StringType)).select("Body")
messages.printSchema

messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

