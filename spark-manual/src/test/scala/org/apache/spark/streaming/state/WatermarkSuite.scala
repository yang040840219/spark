// scalastyle:off
package org.apache.spark.streaming.state

import java.io.File
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.joda.time.format.DateTimeFormat
import org.scalatest.funsuite.AnyFunSuite

/**
  * hello, 2021-03-21 12:00:00
  */
class WatermarkSuite extends AnyFunSuite {
  
  test("watermark update") {
	
	val spark = SparkSession.builder().master("local[4]")
	  .appName("streaming schema evolution")
	  .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
	  .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
	  .config("spark.sql.streaming.schemaInference", "false")
	  .getOrCreate()
	
	spark.sparkContext.setLogLevel("Error")
	
	import spark.implicits._
	
	spark.streams.addListener(new StreamingQueryListener {
	  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
		println("query started")
	  }
	  
	  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
		val batchId = event.progress.batchId
		val eventTime = event.progress.eventTime
		println(s"batchId: $batchId, eventTime: ${eventTime}")
	  }
	  
	  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
		println("query terminated")
	  }
	})
	
	
	val lines = spark.readStream.format("socket").option("host", "localhost")
	  .option("port", "12345").load().as[String]
	
	val words = lines.filter(line => line != null && line.trim.length > 0).map(line => {
	  val data = line.split(",").map(_.trim)
	  val key = data(0)
	  val dateString = data(1)
	  val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
	  val dateTime = format.parseDateTime(dateString)
	  val timestamp = new Timestamp(dateTime.getMillis)
	  (key, timestamp)
	}).toDF("word", "timestamp")
	
	val wordCounts = words.withWatermark("timestamp", "1 minute")
	  .groupBy(window($"timestamp", "1 minute"), $"word")
	  .count()
	
	val checkpointPath = "/opt/data/spark/checkpoint/structed_streaming/watermark"
	
	FileUtils.deleteDirectory(new File(checkpointPath))
	
	// complete mode  watermark 不生效, 只能是 append 、update mode
	val query = wordCounts.writeStream
	  .outputMode("append")
	  .format("console")
	  .option("checkpointLocation", checkpointPath)
	  .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
	  .option("truncate", "false")
	  .start()
	
	query.awaitTermination()
	
  }
  
}
