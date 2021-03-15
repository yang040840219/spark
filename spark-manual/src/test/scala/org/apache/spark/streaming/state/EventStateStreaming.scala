// scalastyle:off
package streaming.org.apache.spark.state

import java.sql.Timestamp
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery, Trigger}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

private[state] case class Event(sessionId: String, timestamp: Timestamp)

private[state] case class SessionInfo(numEvents: Long, start: Long, end: Long) {
  def duration = end - start
}

private[state] case class SessionUpdate(sessionId: String, duration: Long, nums: Long, expired: Boolean)

private[state] object EventStateStreaming {
  
  def main(args: Array[String]): Unit = {
	
	val logger = LoggerFactory.getLogger(EventStateStreaming.getClass)
	
	val spark = SparkSession.builder()
	  .master("local[2]")
	  .appName("EventStateStreaming")
	  .config("spark.local.dir", "/opt/data/spark/streaming/events")
	  .config("spark.sql.shuffle.partitions", "2")
	  .getOrCreate()
	
	//	val lines = spark.readStream
	//	  .format("socket")
	//	  .option("host", "localhost")
	//	  .option("port", "12345")
	//	  .option("includeTimestamp", "true")
	//	  .load()
	
	import spark.implicits._
	
	val lines = spark.readStream
	  .format("kafka")
	  .option("kafka.bootstrap.servers", "localhost:9092")
	  .option("subscribe", "structured-streaming-event")
	  .option("failOnDataLoss", "false")
	  .load()
	  .selectExpr("CAST(value AS STRING)")
	  .as[String]
	
	val events = lines.flatMap(line => {
	  val words = line.split(",")
	  val tm = new Timestamp(Calendar.getInstance().getTimeInMillis)
	  words.map(word => Event(word, tm))
	})
	
	val sessionInfoUpdates = events
	  .groupByKey(event => event.sessionId)
	  .mapGroupsWithState[SessionInfo, Option[SessionUpdate]](GroupStateTimeout.ProcessingTimeTimeout())({
	  case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) => {
		
		val msg =
		  s"""group key: $sessionId,group processingTime:${state.getCurrentProcessingTimeMs()}
		   """.stripMargin.trim()
		
		logger.error(msg)
		
		if (state.hasTimedOut) {
		  logger.error(s"state: $sessionId expired")
		  state.remove()
		  None
		} else {
		  val timestamps = events.map(_.timestamp.getTime).toSeq
		  val updatedSessionInfo = if (state.exists) {
			val oldSessionInfo = state.get
			SessionInfo(
			  oldSessionInfo.numEvents + timestamps.size,
			  oldSessionInfo.start,
			  math.max(oldSessionInfo.end, timestamps.max)
			)
		  } else {
			SessionInfo(timestamps.size, timestamps.min, timestamps.max)
		  }
		  state.update(updatedSessionInfo)
		  state.setTimeoutDuration("1 minute")
		  Some(SessionUpdate(sessionId, updatedSessionInfo.duration, updatedSessionInfo.numEvents, expired = false))
		}
	  }
	}).filter(_.isDefined).map(_.get)
	
	val checkpointPath = "file:///opt/data/spark/streaming/events/checkpoint"
	//	val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
	//	fileSystem.deleteOnExit(new Path(checkpointPath))
	//	fileSystem.close()
	
	val query = sessionInfoUpdates.writeStream
	  .queryName("event-state-streaming")
	  .outputMode(OutputMode.Update())
	  .format("console")
	  .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
	  .option("checkpointLocation", checkpointPath)
	  .start()
	
	logger.error(s"query id: ${query.id}")
	
	manageStreamingQuery(spark)
	
	def manageStreamingQuery(spark: SparkSession): Unit = {
	  val shutdownPath = "file:///opt/data/shutdown/stop_job"
	  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
	  val timeoutInMillSeconds = 30000
	  while (!spark.streams.active.isEmpty) {
		println("check active streaming query ....")
		Try(spark.streams.awaitAnyTermination(timeoutInMillSeconds)) match {
		  case Success(result) => {
			if (result) {
			  println("A streaming query was terminated successfully")
			  spark.streams.resetTerminated()
			}
		  }
		  case Failure(exception) => {
			println(s"Query failed with message :${exception.getMessage}")
			exception.printStackTrace()
			spark.streams.resetTerminated()
		  }
		}
		
		if (checkMarker(shutdownPath, fs)) {
		  spark.streams.active.foreach(query => {
			println(s"Stop streaming query:${query.id}")
			query.stop()
		  })
		  spark.stop()
		  removeMarker(shutdownPath, fs)
		}
	  }
	}
	
	def checkMarker(path: String, fileSystem: FileSystem): Boolean = {
	  fileSystem.exists(new Path(path))
	}
	
	def removeMarker(path: String, fileSystem: FileSystem): Unit = {
	  fileSystem.deleteOnExit(new Path(path))
	}
	
  }
  
}
