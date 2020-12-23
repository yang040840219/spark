// scalastyle:off
package org.apache.spark.sql.shuffle

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{Log, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class ShuffleSuite extends AnyFunSuite with Log {

	val warehouseLocation = "/opt/data/warehouse"

	lazy val spark = SparkSession.builder()
		.master("local[1]")
		.config("spark.sql.warehouse.dir", warehouseLocation)
  	.config("spark.shuffle.compress", "false")
  	.config("spark.local.dir", s"/opt/data/spark/shuffle/${Random.nextInt(1000)}")
  	.config("spark.default.parallelism", "5")
  	.config("spark.sql.parquet.enableVectorizedReader", "false")
		.appName("shuffle suite").getOrCreate()

	test("simple shuffle") {
		val rdd = spark.sparkContext.range(1, 1000000, numSlices = 3)
		val reduceRDD = rdd.map(line => (line % 3, line)).reduceByKey((x1, x2) => x1 + x2)
		print(reduceRDD.toDebugString)
		val result = reduceRDD.collect()
		println(result.mkString(","))
		TimeUnit.DAYS.sleep(1)
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
