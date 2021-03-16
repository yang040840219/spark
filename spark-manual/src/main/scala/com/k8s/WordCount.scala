// scalastyle:off
package com.k8s

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object WordCount {
  
  def main(args: Array[String]): Unit = {
	val spark = SparkSession.builder().getOrCreate()
	import spark.implicits._
	val df = (0 until 100000).toDF("id").selectExpr("id % 5 as key", "id%10 as value")
	  .groupBy("key").agg(count("value1").as("cnt"))
	  .repartition(1).mapPartitions(iter => {
	  iter.map(row => {
		(row.getAs[Int]("key"), row.getAs[Long]("cnt"))
	  })
	}).toDF("key", "value")
	df.show()
	spark.stop()
  }
  
}
