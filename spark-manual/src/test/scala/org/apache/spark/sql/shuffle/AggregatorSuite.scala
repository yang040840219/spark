// scalastyle:off
package org.apache.spark.sql.shuffle

import java.util.{Comparator, Properties}

import com.google.common.hash.Hashing
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.apache.spark.util.collection.{PartitionedAppendOnlyMap, SizeTracker}
import org.apache.spark.{Aggregator, HashPartitioner, TaskContext, TaskContextImpl}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class AggregatorSuite extends AnyFunSuite {
  
  test("simple aggregate") {
	
	val spark = SparkSession.builder().master("local[1]").getOrCreate()
	
	val taskMemoryManager = new TaskMemoryManager(spark.sparkContext.env.memoryManager, 0L)
	val metricsSystem = spark.sparkContext.env.metricsSystem
	
	// K -> V -> C
	
	val createCombiner = (x: Int) => List(x)
	
	val mergeValue = (x1: List[Int], x2: Int) => {
	  x1.+:(x2)
	}
	val mergeCombiners = (x1: List[Int], x2: List[Int]) => {
	  val listBuffer = new ListBuffer[Int]()
	  listBuffer.append(x1: _*)
	  listBuffer.append(x2: _*)
	  listBuffer.distinct.toList
	}
	
	val taskContext = new TaskContextImpl(0, 0, 0, 0, 0, taskMemoryManager, new Properties, metricsSystem)
	TaskContext.setTaskContext(taskContext)
	
	/**
	  * createCombiner => (V) -> C     V 是输入输入的类型, createCombiner 是对输入进行转换,转后之后的类型是进行后续计算的基础
	  * mergeCombiner  => (C, V) -> C  定义计算合并类型和新值之间的计算逻辑
	  * mergeCombiner  => (C, C) -> C  计算两个合并类型之间的计算逻辑
	  */
	val aggregator = Aggregator[String, Int, List[Int]](
	  createCombiner, mergeValue, mergeCombiners
	)
	
	// 模拟 map task 数据
	val iterator = Seq[(String, Int)](
	  ("a", 1), ("a", 1),
	  ("b", 1), ("b", 3), ("b", 3)).toIterator
	
	// 非 map-side combine
	val r0 = aggregator.combineValuesByKey(iterator, taskContext)
	
	println(s"r0:${r0.mkString(",")}")
	
	// 模拟 reduce task 数据
	val combineIterator = Seq[(String, List[Int])](
	  ("a", List(1, 2, 3)), // map task 1
	  ("a", List(4, 5)), // map task 1
	  ("a", List(2, 3, 4)), // map task 2
	  ("b", List(1))
	).toIterator
	
	// 在 map-side combine
	val r1 = aggregator.combineCombinersByKey(combineIterator, taskContext)
	println(s"r1:${r1.mkString(",")}")
	
  }
  
  test("merge map") {
	val createCombiner = (x: Int) => List(x)
	val mergeValue = (x1: List[Int], x2: Int) => {
	  x1.:+(x2)
	}
	val mergeCombiners = (x1: List[Int], x2: List[Int]) => {
	  val listBuffer = new ListBuffer[Int]()
	  listBuffer.append(x1: _*)
	  listBuffer.append(x2: _*)
	  listBuffer.distinct.toList
	}
	
	val iterator = Seq[(String, Int)](
	  ("a", 1), ("a", 1), ("a", 2),
	  ("b", 1), ("b", 3), ("b", 3)).toIterator
	
	val partitioner = new HashPartitioner(2)
	
	val map = new mutable.LinkedHashMap[(Int, String), List[Int]]()
	
	def insertAll(records: Iterator[Product2[String, Int]]): Unit = {
	  while (records.hasNext) {
		val kv = records.next()
		val partition = partitioner.getPartition(kv._1)
		val update = (hadValue: Boolean, oldValue: List[Int]) => {
		  if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
		}
		val key = (partition, kv._1)
		val exists = map.contains(key)
		val value = update(exists, map.getOrElse(key, Nil))
		map.put(key, value)
	  }
	}
	
	insertAll(iterator)
	println(map)
  }
  
  test("null AnyRef") {
	println(null.asInstanceOf[AnyRef])
	
	def rehash(h: Int) = Hashing.murmur3_32().hashInt(h).asInt()
	
	val r = (0 until 1000).map(x => rehash(x) & 7).groupBy(x => x)
	r.foreach(x => println(x._1, x._2.size))
  }
  
  test("next power2") {
	def nextPowerOf2(n: Int): Int = {
	  val highBit = Integer.highestOneBit(n)
	  if (highBit == n) n else highBit << 1
	}
	
	println(nextPowerOf2(10))
  }
  
  test("size tracker") {
	
	class Cache extends SizeTracker {
	  
	  private val cache = new mutable.HashMap[String, Int]()
	  
	  def put(key: String, value: Int): Unit = {
		cache.put(key, value)
		super.afterUpdate()
	  }
	  
	  def get(key: String): Option[Int] = {
		cache.get(key)
	  }
	  
	}
	
	val cache = new Cache()
	cache.put("a", 1)
	println(s"size:${cache.estimateSize()}")
	cache.put("b", 1)
	println(s"size:${cache.estimateSize()}")
	cache.put("b", 1)
	println(s"size:${cache.estimateSize()}")
	cache.put("c", 1)
	println(s"size:${cache.estimateSize()}")
	
	val map = new mutable.HashMap[String, Int]()
	println(SizeEstimator.estimate(map))
	map.put("a", 1)
	println(SizeEstimator.estimate(map))
	
	println(SizeEstimator.estimate(Array[Byte](1, 2, 3, 4, 5, 6)))
	
  }
  
  test("partitioned append only map") {
	val map = new PartitionedAppendOnlyMap[String, List[Int]]
	val createCombiner = (x: Int) => List(x)
	val mergeValue = (x1: List[Int], x2: Int) => {
	  x1.:+(x2)
	}
	
	var kv: Product2[String, Int] = null
	val update = (hadValue: Boolean, oldValue: List[Int]) => {
	  if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
	}
	
	val iterator = Seq[(String, Int)](
	  ("a", 1), ("a", 1),
	  ("b", 1), ("b", 3), ("b", 3),
	  ("c", 3), ("c", 2),
	  ("d", 2), ("d", 1)).toIterator
	
	val partitioner = new HashPartitioner(2)
	
	def getPartition(v: String) = partitioner.getPartition(v)
	
	while (iterator.hasNext) {
	  kv = iterator.next()
	  map.changeValue((getPartition(kv._1), kv._1), update)
	}
	
	val keyComparator = Ordering[String]
	
	def partitionKeyComparator[String](keyComparator: Comparator[String]): Comparator[(Int, String)] = {
	  (a: (Int, String), b: (Int, String)) => {
		val partitionDiff = a._1 - b._1
		if (partitionDiff != 0) {
		  partitionDiff
		} else {
		  keyComparator.compare(a._2, b._2)
		}
	  }
	}
	
	val mapIter = map.partitionedDestructiveSortedIterator(Some(keyComparator))
	
	// val mapIter = map.destructiveSortedIterator(partitionKeyComparator(keyComparator))
	
	while (mapIter.hasNext) {
	  println(mapIter.next())
	}
	
  }
  
}
