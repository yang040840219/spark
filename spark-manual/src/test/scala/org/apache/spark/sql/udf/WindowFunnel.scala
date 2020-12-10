// scalastyle:off
package org.apache.spark.sql.udf

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable.ListBuffer

/**
	* 类似 clickhouse 漏斗函数, 根据group by 返回窗口中数据最大的到达数组最大位置
	*/

case class Event(eventId:String, time:Long) {
	override def hashCode(): Int = (eventId + time.toString).hashCode

	override def equals(obj: Any): Boolean = {
		eventId.equals(obj.asInstanceOf[Event].eventId) && time == obj.asInstanceOf[Event].time
	}
}

/**
	* @param events 保存事件信息
	* @param eventSeq 要匹配的事件序列
	* @param duration 两个事件之间的时长
	*/
case class EventBuffer(var events:ListBuffer[Event], var eventSeq:String, var duration:Int)

/**
	* (String, String, String, Int)
	*
	*/
object WindowFunnel extends Aggregator[(String, String, String, Int), EventBuffer, Int] {
	/**
		* A zero value for this aggregation. Should satisfy the property that any b + zero = b.
		*
		* @since 1.6.0
		*/
	override def zero: EventBuffer = EventBuffer(ListBuffer[Event](), "", 0)

	/**
		* Combine two values to produce a new value.  For performance, the function may modify `b` and
		* return it instead of constructing new object for b.
		*
		* @since 1.6.0
		*/
	override def reduce(b: EventBuffer, a: (String, String, String, Int)): EventBuffer = {
		val event = Event(a._1, a._2.toLong)
	  b.events.append(event)
		b.eventSeq = a._3
		b.duration = a._4
		b
	}

	/**
		* Merge two intermediate values.
		*
		* @since 1.6.0
		*/
	override def merge(b1: EventBuffer, b2: EventBuffer): EventBuffer = {
		b2.events.foreach(event => b1.events.append(event))
		b1.duration = b2.duration
		b1.eventSeq = b2.eventSeq
		b1
	}

	/**
		* Transform the output of the reduction.
		*
		* @since 1.6.0
		*/
	override def finish(reduction: EventBuffer): Int = {
		val events = reduction.events
		val eventSeq = reduction.eventSeq.split(",").toList.map(event => event.trim)
		val level = if(eventSeq.isEmpty){
			0
		} else {
			val duration = reduction.duration //  传递的时长，用来判断两个事件的间隔
			val sortEvents = events.sortWith({case (e1, e2) => {e1.time > e2.time}})
			val alreadyEvents = sortEvents.map(e => e.eventId).distinct.toSet
			eventSeq.toSet.&(alreadyEvents).size
		}
		level
	}

	/**
		* Specifies the `Encoder` for the intermediate value type.
		*
		* @since 2.0.0
		*/
	override def bufferEncoder: Encoder[EventBuffer] = Encoders.product

	/**
		* Specifies the `Encoder` for the final output value type.
		*
		* @since 2.0.0
		*/
	override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}
