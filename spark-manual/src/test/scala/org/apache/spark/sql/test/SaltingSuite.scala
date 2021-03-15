// scalastyle:off
package org.apache.spark.sql.test

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Salting {
  def main(args: Array[String]): Unit = {
	
	def saltedJoin(df: DataFrame, buildDF: DataFrame,
				   joinExpression: Column, joinType:String,
				   salt:Int): DataFrame = {
		val tmpDF = buildDF.withColumn("slt_range",
		  array(Range(0, salt).toList.map(lit):_*))
	  
	  null
	}
	
	
	val spark = SparkSession.builder().master("local[2]").getOrCreate()
	
	val df1 = spark.createDataFrame(Seq((1, "a"), (1, "b"), (2, "a")), )
	
  }
}
