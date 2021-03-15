// scalastyle:off
package org.apache.spark.sql.test

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class DistinctSuite extends SparkFunSuite {
  
  val spark = SparkSession.builder().master("local[2]").getOrCreate()
  
  /**
	* combineByKeyWithClassTag
	*/
  test("rdd distinct") {
	val rdd = spark.sparkContext.range(0, 10000, numSlices = 2).map(x => (x % 10, x))
	// 转换为 (x,_1) =>  reduceByKey(_ + _)
	val count = rdd.countByKey()
	count.foreach(println)
  }
  
  
  test("sql distinct") {
	import spark.implicits._
	val df = (0 until 10000).map(x => (x % 10, x % 3)).toDF("id", "value")
	// 使用两阶段hash 的方式  hash(id, value) -->  hash(id) 避免了OOM
	val result = df.groupBy(col("id")).agg(countDistinct(col("value")).as("cnt"))
	result.explain(true)
	result.show()
  }
  
}
