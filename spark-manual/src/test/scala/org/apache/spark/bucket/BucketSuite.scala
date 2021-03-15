// scalastyle:off
package org.apache.spark.bucket

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class BucketSuite extends AnyFunSuite {
  
  val spark = SparkSession.builder()
	.appName("bucket suite").master("local[3]")
	.config("spark.driver.host", "localhost")
	.config("spark.sql.adaptive.enabled", "true")
	.config("spark.sql.adaptive.logLevel", "info")
	.config("spark.sql.adaptive.coalescePartitions.enabled", "false")
	.config("spark.sql.adaptive.localShuffleReader.enabled", "false") // true local shuffle reader
	.config("spark.sql.autoBroadcastJoinThreshold", "-1")
	.config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", 0.00000001d)
	.config("spark.sql.shuffle.partitions", 10)
    .config("spark.sql.warehouse.dir", "/opt/data/spark/bucket")
	.getOrCreate()
  
  
  val input4Path = "file:///opt/data/spark/bucket/input4"
  val input5Path = "file:///opt/data/spark/bucket/input5"
  
  test("write bucket data") {
	import spark.implicits._
	val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
	fileSystem.deleteOnExit(new Path(input4Path))
	fileSystem.deleteOnExit(new Path(input5Path))
	fileSystem.close()
	val input4 = spark.sparkContext.parallelize((1 to 2000).map(x => (x, x.toString)), 3).toDF("key", "value")
	val input5 = spark.sparkContext.parallelize((1 to 2000).map(x => (x, x.toString)), 5).toDF("key", "value")
	input4.write.bucketBy(2, "key").sortBy("value").mode(SaveMode.Overwrite).saveAsTable("input4")
	input5.write.bucketBy(2, "key").sortBy("value").mode(SaveMode.Overwrite).saveAsTable("input5")
	
	val describeSQL = spark.sql("desc extended input4")
	
	describeSQL.show(truncate = false)
	
	val sql =
	  s"""
		 |select t1.key, t1.value, t2.key, t2.value
		 |from input4 t1 left join input5 t2
		 |on t1.key = t2.key
	   """.stripMargin
	
	val result = spark.sql(sql)
	result.show(10)
	TimeUnit.DAYS.sleep(1)
  }
  
  
  test("smj") {
	val input4 = spark.read.parquet(input4Path)
	val input5 = spark.read.parquet(input5Path)
	val result = input4.join(input5, input4("key") === input5("key"), "left")
	result.explain(true)
	TimeUnit.DAYS.sleep(1)
  }
  
}
