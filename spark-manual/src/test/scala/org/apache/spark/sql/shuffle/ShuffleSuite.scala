// scalastyle:off
package org.apache.spark.sql.shuffle

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Log, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class ShuffleSuite extends AnyFunSuite with Log {
  
  val warehouseLocation = "/opt/data/warehouse"
  
  val shufflePath = "/opt/data/spark/shuffle"
  
  lazy val spark = SparkSession.builder()
	.master("local[1]")
	.config("spark.sql.warehouse.dir", warehouseLocation)
	.config("spark.shuffle.compress", "false")
	.config("spark.local.dir", s"$shufflePath/${Random.nextInt(1000)}")
	.config("spark.default.parallelism", "5")
	.config("spark.sql.parquet.enableVectorizedReader", "false")
	.config("spark.executor.heartbeatInterval", "100000000")
	.config("spark.network.timeout", "100000001")
	.config("spark.shuffle.compress", "false")
	.config("spark.shuffle.sort.bypassMergeThreshold", "1")
	.config("spark.shuffle.spill.numElementsForceSpillThreshold", "100")
	.appName("shuffle suite").getOrCreate()
  
  FileUtils.deleteDirectory(new File(shufflePath))
  
  
  test("simple shuffle map-side aggregation") {
	val rdd = spark.sparkContext.range(1, 1000, numSlices = 3)
	val reduceRDD = rdd.map(line => (Random.nextInt(line.toInt) % 50, line)).reduceByKey(_ + _, 4)
	print(reduceRDD.toDebugString)
	val result = reduceRDD.collect()
	result.foreach(println)
	// TimeUnit.DAYS.sleep(1)
  }
  
  test("simple shuffle without map-side aggregation") {
	val rdd = spark.sparkContext.range(1, 1000, numSlices = 3)
	val reduceRDD = rdd.map(line => (Random.nextInt(line.toInt) % 50, line))
	  .groupByKey(4).map(x => (x._1, x._2.size))
	print(reduceRDD.toDebugString)
	val result = reduceRDD.collect()
	result.foreach(println)
	// TimeUnit.DAYS.sleep(1)
  }
  
  val path = "file:///opt/data/parquet/test"
  
  test("write parquet") {
	import spark.implicits._
	val rdd = spark.sparkContext.range(1, 100, numSlices = 3)
	val df = rdd.map(x => (x, x.toString + Random.nextInt(x.toInt))).toDF("id", "value")
	df.write.mode("overwrite").format("parquet").save(path)
  }
  
  test("read parquet") {
	val df = spark.read.parquet(path)
	val cnt = df.count()
	print(s"cnt: $cnt")
  }
}
