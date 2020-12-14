// scalastyle:off
package org.apache.spark.sql.shuffle

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{Log, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class ShuffleSuite extends AnyFunSuite with Log {

	val warehouseLocation = "/opt/data/warehouse"

	lazy val spark = SparkSession.builder()
		.master("local[3]")
		.config("spark.sql.warehouse.dir", warehouseLocation)
  	.config("spark.shuffle.compress", "false")
  	.config("spark.local.dir", s"/opt/data/spark/shuffle/${Random.nextInt(1000)}")
  	.config("spark.default.parallelism", "5")
		.appName("shuffle suite").getOrCreate()

	test("simple shuffle") {
		val rdd = spark.sparkContext.range(1, 1000000, numSlices = 3)
		val reduceRDD = rdd.map(line => (line % 3, line)).reduceByKey((x1, x2) => x1 + x2)
		print(reduceRDD.toDebugString)
		val result = reduceRDD.collect()
		println(result.mkString(","))
		TimeUnit.DAYS.sleep(1)
	}


}
