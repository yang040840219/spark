// scalastyle:off
package org.apache.spark.sql.shuffle

import java.io.{File, FileInputStream, FileOutputStream}

import com.esotericsoftware.kryo.io.Output
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.scalatest.funsuite.AnyFunSuite

class SerializerSuite extends AnyFunSuite {
  
  val conf = new SparkConf().set("spark.shuffle.compress", "false")
  
  val iterator = Seq[(String, Int)](
	("a", 1), ("a", 1),
	("b", 1), ("b", 3), ("b", 3),
	("c", 3), ("c", 2),
	("d", 2), ("d", 1)).toIterator
  
  
  test("serialize object") {
	val serializer = new JavaSerializer(conf)
	val serializerInstance = serializer.newInstance()
	val u1 = Student(1L, "a", 10)
	val buffer = serializerInstance.serialize[Student](u1)
	val u2 = serializerInstance.deserialize[Student](buffer)
	println(u2)
  }
  
  val path = "/opt/data/serializer/1.txt"
  
  test("serialize stream") {
	val serializer = new JavaSerializer(conf)
	val serializerInstance = serializer.newInstance()
	val fileOutputStream = new FileOutputStream(new File(path))
	val wrapFileOutputStream = serializerInstance.serializeStream(fileOutputStream)
	wrapFileOutputStream.writeAll(iterator)
	
	val fileInputStream = new FileInputStream(new File(path))
	val wrapFileInputStream = serializerInstance.deserializeStream(fileInputStream)
	val serIter = wrapFileInputStream.asIterator.asInstanceOf[Iterator[(String, Int)]]
	println(serIter.mkString(","))
  }
  
  test("serialize stream position") {
	val serializer = new KryoSerializer(conf)
	val serializerInstance = serializer.newInstance()
	val bufferSize = 20
	val output = new Output(bufferSize)
	val wrapStream = serializerInstance.serializeStream(output)
	println(s"position:${output.position()}")
	wrapStream.writeKey("a")
	wrapStream.writeValue(1)
	wrapStream.flush()
	println(s"position:${output.position()}")
	wrapStream.writeKey("a")
	wrapStream.writeValue(1)
	wrapStream.flush()
	println(s"position:${output.position()}")
  }
  
}
