// scalastyle:off
package org.apache.spark.sql.test

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{Log, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class FunctionSuite extends AnyFunSuite with Log {
  
  test("row_number") {
	
	val spark = SparkSession.builder().master("local[2]")
	  .config("spark.sql.shuffle.partitions", "2")
	  .getOrCreate()
	import spark.implicits._
	
	val df = Seq(("a", 1), ("b", 2), ("a", 2), ("a", 2), ("b", 3), ("c", 1)).toDF("dep", "age")
	
	df.createTempView("t_1")
	
	// 使用 group by dep, age 结果不同， 先执行 group by 后执行 row_number
	val sql =
	  s"""
		 |select
		 |	 dep,
		 |   age
		 |from
		 |(select
		 |	 dep,
		 |   age,
		 |   row_number() over (partition by dep order by age desc) as rn
		 |from t_1
		 |group by dep, age) t_2
		 |where rn = 2
	   """.stripMargin
	
	val result = spark.sql(sql)
	
	result.explain(true)
	
	result.show()
	
	TimeUnit.DAYS.sleep(1)
 
	/**
	  * AdaptiveSparkPlan isFinalPlan=false
	  * +- Project [dep#7, age#8]
	  * 	+- Filter (isnotnull(rn#11) AND (rn#11 = 2))
	  * 		+- Window [row_number() windowspecdefinition(dep#7, age#8 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#11], [dep#7], [age#8 DESC NULLS LAST]
	  * 			+- Sort [dep#7 ASC NULLS FIRST, age#8 DESC NULLS LAST], false, 0
	  * 				+- Exchange hashpartitioning(dep#7, 200), ENSURE_REQUIREMENTS, [id=#29]
	  * 					+- HashAggregate(keys=[dep#7, age#8], functions=[], output=[dep#7, age#8])
	  * 						+- Exchange hashpartitioning(dep#7, age#8, 200), ENSURE_REQUIREMENTS, [id=#26]
	  * 							+- HashAggregate(keys=[dep#7, age#8], functions=[], output=[dep#7, age#8])
	  * 								+- LocalTableScan [dep#7, age#8]
	  */
	
  }
}
