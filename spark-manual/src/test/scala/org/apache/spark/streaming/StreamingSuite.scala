// scalastyle:off

package org.apache.spark.streaming

import java.util.concurrent.TimeUnit
import java.util.{Locale, Properties, TimeZone}

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random


class StreamingSuite extends SparkFunSuite {
  
  TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"))
  // Add Locale setting
  Locale.setDefault(Locale.CHINA)
  
  
  test("continuous streaming") {
	val spark = SparkSession.builder().master("local[2]").appName("test")
	  .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
	  .config("spark.network.timeout", 100000001)
	  .config("spark.executor.heartbeatInterval", 100000000)
	  .config("spark.storage.blockManagerSlaveTimeoutMs", 100000001)
	  .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
	  .getOrCreate()
	
	import spark.implicits._
	
	val lines = spark.readStream.format("socket").option("host", "localhost")
	  .option("port", "9999").load()
	
	def f(x: String): Boolean = StringUtils.isNoneBlank(x)
	
	val words = lines.as[String]
	  .filter(f _)
	  .map(line => {
		val item = line.split(",")
		val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
		val timestamp = new java.sql.Timestamp(fmt.parseDateTime(item(1)).getMillis)
		(item(0), timestamp)
	  }).toDF("event", "time")
	
	val counts = words
	  .coalesce(1)
	  .groupBy($"event").count()
	
	val checkpointPath = s"file:///data/delta/checkpoint/words/${Random.nextInt(10000)}"
	
	log.warn(s"checkpointPath:$checkpointPath")
	
	val query = counts.writeStream
	  //        .foreach(new ForeachWriter[Row] {
	  //
	  //            override def process(value: Row) = {
	  //                // scalastyle:off
	  //                println(value.toString)
	  //            }
	  //
	  //            override def close(errorOrNull: Throwable) = {
	  //                println("close")
	  //            }
	  //
	  //            override def open(partitionId: Long, epochId: Long) = {
	  //                println(s"partitionId:$partitionId, epochId:$epochId")
	  //                true
	  //            }
	  //        })
	  .format("console")
	  .outputMode(OutputMode.Complete())
	  .trigger(Trigger.Continuous(60.seconds))
	  .option("checkpointLocation", checkpointPath)
	  .option("truncate", "false")
	  .queryName("world_count")
	  .start()
	
	
	query.awaitTermination()
	
	spark.stop()
	
	
  }
  
  test("structured streaming") {
	val spark = SparkSession.builder().master("local[2]").appName("test")
	  .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
	  .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
	  .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
	  .config("spark.network.timeout", 100000001)
	  .config("spark.executor.heartbeatInterval", 100000000)
	  .config("spark.storage.blockManagerSlaveTimeoutMs", 100000001)
	  .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
	  .getOrCreate()
	
	import spark.implicits._
	
	spark.streams.addListener(new StreamingQueryListener {
	  
	  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
	  
	  
	  override def onQueryProgress(event: QueryProgressEvent): Unit = {
		log.warn(event.progress.eventTime.asScala.mkString(","))
	  }
	  
	  
	  override def onQueryStarted(event: QueryStartedEvent): Unit = {}
	})
	
	val lines = spark.readStream.format("socket").option("host", "localhost")
	  .option("port", "9999").load()
	
	def f(x: String): Boolean = StringUtils.isNoneBlank(x)
	
	val words = lines.as[String]
	  .filter(f _)
	  .map(line => {
		TimeUnit.SECONDS.sleep(10)
		val item = line.split(",")
		val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
		val timestamp = new java.sql.Timestamp(fmt.parseDateTime(item(1)).getMillis)
		(item(0), timestamp)
	  }).toDF("event", "time")
	val counts = words
	// .withWatermark("time", "5 minutes")
	// .groupBy(window($"time", "3 minutes"), $"event").count()
	
	val checkpointPath = s"file:///data/delta/checkpoint/words/${Random.nextInt(10000)}"
	
	log.warn(s"checkpointPath:$checkpointPath")
	
	val query = counts.writeStream
	  .outputMode(OutputMode.Append())
	  .format("console")
	  .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
	  .option("checkpointLocation", checkpointPath)
	  .option("truncate", "false")
	  .queryName("world_count")
	  .start()
	
	
	query.awaitTermination()
	
	spark.stop()
  }
  
  
  // scalastyle:off
  test("timestamp") {
	println(TimeZone.getDefault)
	val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
	val datetime = fmt.parseDateTime("2020-07-15 20:20:00")
	println(datetime)
	val timestamp = new java.sql.Timestamp(datetime.getMillis)
	println(timestamp)
  }
  
  
  test("kafka structured streaming source provider") {
	val spark = SparkSession.builder().master("local[2]").appName("kafka")
	  .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
	  .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
	  .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
	  .config("spark.network.timeout", 100000001)
	  .config("spark.executor.heartbeatInterval", 100000000)
	  .config("spark.storage.blockManagerSlaveTimeoutMs", 100000001)
	  .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
	  .getOrCreate()
	
	val properties = new Properties()
	properties.setProperty("bootstrap.servers", "localhost:9092")
	properties.setProperty("group.id", "kafka-client-v1")
	properties.setProperty("key.deserializer",
	  "org.apache.kafka.common.serialization.StringDeserializer")
	properties.setProperty("value.deserializer",
	  "org.apache.kafka.common.serialization.StringDeserializer")
	val consumer = new KafkaConsumer[String, String](properties)
	
	spark.streams.addListener(new KafkaLagWriter(consumer))
	
	import spark.implicits._
	
	val lines = spark.readStream
	  .format("kafka")
	  .option("kafka.bootstrap.servers", "localhost:9092")
	  .option("subscribe", "client-bi-log")
	  .option("startingOffsets", "latest")
	  .option("failOnDataLoss", "false")
	  .option("maxOffsetsPerTrigger", 50000)
	  .option("group.id", "kafka-client-v1")
	  .load()
	  .selectExpr("CAST(value as STRING) as value")
	  .as[String]
	  .map(line => {
		val item = line.split(",")
		val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
		val timestamp = new java.sql.Timestamp(fmt.parseDateTime(item(1)).getMillis)
		(item(0), timestamp)
	  }).toDF("event", "time")
	
	val counts = lines
	//        .withWatermark("time", "5 minutes")
	//        .groupBy(window($"time", "3 minutes"), $"event").count()
	
	val checkpointPath = s"file:///data/checkpoint/kafka/${Random.nextInt(10000)}"
	
	val query = counts.writeStream
	  .outputMode(OutputMode.Append())
	  .format("console")
	  .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
	  .option("checkpointLocation", checkpointPath)
	  .option("truncate", "false")
	  .queryName("kafka")
	  .start()
	
	query.awaitTermination()
	
	spark.stop()
	
  }
  
  test("streaming schema evolution") {
	val spark = SparkSession.builder().master("local[4]")
	  .appName("streaming schema evolution")
	  .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
	  .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
	  .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
	  .config("spark.sql.streaming.schemaInference", "false")
	  .getOrCreate()
	
	import spark.implicits._
	
	val lines = spark.readStream.format("socket").option("host", "localhost")
	  .option("port", "9999").load().as[String]
	
	val schema = StructType(Seq(
	  StructField("name", StringType),
	  StructField("age", IntegerType),
	  StructField("age", IntegerType)
	))
	
	val df = lines.select(from_json(col("value"), schema).as("data"))
	  .select("data.*")
	
	val filter = df
	
	val query = filter.writeStream
	  .foreachBatch({
		(batchDF: DataFrame, batchId: Long) => {
		  batchDF.show(truncate = false)
		}
	  })
	  .start()
	
	query.awaitTermination()
	
	
  }
  
}
