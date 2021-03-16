// scalastyle:off
package org.apache.spark.sql.shuffle

import java.io.{DataInputStream, File}
import java.nio.channels.Channels
import java.nio.file.Files

import org.apache.spark.network.buffer.FileSegmentManagedBuffer
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerManager}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.{HashPartitioner, SparkConf}
import org.scalatest.funsuite.AnyFunSuite


private[shuffle] case class Student(id: Long, name: String, age: Int)


class ShuffleFileSuite extends AnyFunSuite {
  
  val shufflePath = "/opt/data/spark/shuffle"
  
  val conf = new SparkConf().set("spark.shuffle.compress", "false")
  
  
  val tmp = "/907/blockmgr-c1a1172f-1df2-42d7-bc7f-382210246bfb"
  
  test("read shuffle data") {
	val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
	val serializer = new JavaSerializer(conf)
	val serializerManager = new SerializerManager(serializer, conf, None)
	val kryoSerializer = new KryoSerializer(conf).newInstance()
	
	val shuffleBlockId = ShuffleBlockId(0, 2, 0)
	
	val indexPaths = Seq[String]("/30/shuffle_0_0_0.index")
	val dataPaths = Seq[String]("/0c/shuffle_0_0_0.data")
	
	val merges = indexPaths.zip(dataPaths)
	merges.foreach({ case (index, file) => {
	  val indexPath = shufflePath + tmp + index
	  val indexFile = new File(indexPath)
	  val channel = Files.newByteChannel(indexFile.toPath)
	  val in = new DataInputStream(Channels.newInputStream(channel))
	  val startReduceId = 2
	  val endReduceId = 3
	  channel.position(startReduceId * 8L)
	  val startOffset = in.readLong()
	  channel.position(endReduceId * 8L)
	  val endOffset = in.readLong()
	  channel.close()
	  in.close()
	  println(s"startOffset:$startOffset, endOffset:$endOffset")
	  val dataPath = shufflePath + tmp + file
	  val dataFile = new File(dataPath)
	  val managedBuffer = new FileSegmentManagedBuffer(transportConf, dataFile,
		startOffset, endOffset - startOffset)
	  val input = managedBuffer.createInputStream()
	  val wrapStream = serializerManager.wrapStream(shuffleBlockId, input)
	  val recordIter = kryoSerializer.deserializeStream(wrapStream).asKeyValueIterator
	  val iter = recordIter.asInstanceOf[Iterator[Product2[Int, Int]]]
	  iter.map(x => x._1).toSet.foreach(println)
	}
	})
	
  }
  
  
  test("partitioner") {
	val partitioner = new HashPartitioner(4)
	(0 until 10).map(key => {
	  val partition = partitioner.getPartition(key)
	  (partition, key)
	}).groupBy(_._1)
	  .foreach(item => println(s"${item._1} -> ${item._2.map(_._2).mkString(",")}"))
  }
  
}
