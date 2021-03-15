// scalastyle:off
package org.apache.spark.streaming

import java.util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

/**
  * 2020/5/15
  */
class KafkaLagWriter(consumer: KafkaConsumer[String, String]) extends StreamingQueryListener {
  
  val om = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  om.registerModule(DefaultScalaModule)
  
  override def onQueryStarted(event: QueryStartedEvent): Unit = {
	
  }
  
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
	event.progress.sources.foreach(source => {
	  val endOffset = source.endOffset
	  val jsonOffsets = om.readValue(endOffset, classOf[Map[String, Map[String, Int]]])
	  val topicPartitionMap = new util.HashMap[TopicPartition, OffsetAndMetadata]()
	  jsonOffsets.foreach({ case (topic, offsets) => {
		offsets.foreach({ case (partition, offset) => {
		  val topicPartition = new TopicPartition(topic, partition.toInt)
		  val offsetAndMetadata = new OffsetAndMetadata(offset.toLong)
		  topicPartitionMap.put(topicPartition, offsetAndMetadata)
		}
		})
	  }
	  })
	  consumer.commitSync(topicPartitionMap)
	})
  }
  
  
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
	
  }
}
