// scalastyle:off
package org.apache.spark.sql.test

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class SaltingSuite extends SparkFunSuite {
  
  /**
	* 带有 salt 的 DataFrame Join
	* @param df
	* @param buildDF
	* @param joinExpression
	* @param joinType
	* @param salt
	* @return
	*/
  def saltedJoin(df: DataFrame, buildDF: DataFrame,
				 joinExpression: Column, joinType: String,
				 salt: Int): DataFrame = {
	// 相当于扩充维度表
	val tmpDF = buildDF.withColumn("salt_range",array(Range(0, salt).toList.map(lit): _*))
	val tableDF = tmpDF.withColumn("salt_ratio", explode(col("salt_range")))
	
	// 对事实表按照 salt_ratio 更新各行, 次列用于后续的join分区
	val streamDF = df.withColumn("salt_ratio", monotonically_increasing_id() % salt)
	
	val saltedExpr = streamDF("salt_ratio") === tableDF("salt_ratio") && joinExpression
	streamDF.join(tableDF, saltedExpr, joinType)
  }
  
  test("salt join") {
	val spark = SparkSession.builder().master("local[2]").getOrCreate()
	import spark.implicits._
	val df1 = Seq((1, "x"), (1, "y"), (1, "z"), (2, "m")).toDF("id", "name")
	val df2 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "name")
	val resultDF = saltedJoin(df1, df2, df1("id") === df2("id"), "left", 3)
	resultDF.show()
  }
  
  test("simple") {
	val spark = SparkSession.builder().master("local[2]").getOrCreate()
	import spark.implicits._
	// 事实表 id=1 数据倾斜
	val df1 = Seq((1, "x"), (1, "y"), (1, "z"), (1, "m"), (1, "n"), (2, "o")).toDF("id", "name")
	// 维度表
	val df2 = Seq((1, "a"), (2, "b"), (3, "c"), (2, "b")).toDF("id", "name")
	val salt = 5
	
	val tableDF = df2.withColumn("slt_range",array(Range(0, salt).toList.map(lit): _*))
	tableDF.show()
	val xDF = tableDF.withColumn("salt_ratio_s", explode(col("slt_range")))
	xDF.show()
	
	val streamDF = df1.withColumn("salt_ratio", monotonically_increasing_id() % salt)
	streamDF.show()
	
	val saltedExpr = streamDF("salt_ratio") === xDF("salt_ratio_s") && df1("id") === df2("id")
	
	val resultDF = streamDF.join(xDF, saltedExpr, "left")
	
	resultDF.show()
  }
}
