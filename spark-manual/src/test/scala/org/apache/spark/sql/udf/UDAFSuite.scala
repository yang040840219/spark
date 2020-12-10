// scalastyle:off
package org.apache.spark.sql.udf

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

case class Average(var sum: Long, var count:Long)

object MyAverage extends Aggregator[Long, Average, Double] {
	/**
		* A zero value for this aggregation. Should satisfy the property that any b + zero = b.
		*
		* @since 1.6.0
		*/
	override def zero: Average = Average(0L, 0L)

	/**
		* Combine two values to produce a new value.  For performance, the function may modify `b` and
		* return it instead of constructing new object for b.
		*
		* @since 1.6.0
		*/
	override def reduce(b: Average, a: Long): Average = {
		b.sum += a
		b.count += 1
		b
	}

	/**
		* Merge two intermediate values.
		*
		* @since 1.6.0
		*/
	override def merge(b1: Average, b2: Average): Average = {
		b1.sum += b2.sum
		b1.count += b2.count
		b1
	}

	/**
		* Transform the output of the reduction.
		*
		* @since 1.6.0
		*/
	override def finish(reduction: Average): Double = {
		reduction.sum.toDouble / reduction.count.toDouble
	}

	/**
		* Specifies the `Encoder` for the intermediate value type.
		*
		* @since 2.0.0
		*/
	override def bufferEncoder: Encoder[Average] = Encoders.product

	/**
		* Specifies the `Encoder` for the final output value type.
		*
		* @since 2.0.0
		*/
	override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

class UDAFSuite  extends SparkFunSuite{

	val warehouseLocation = "/opt/data/warehouse"

	test("average") {
		val spark = SparkSession.builder().master("local[3]")
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.getOrCreate()

		spark.udf.register("myAverage", functions.udaf(MyAverage))

		val df = spark.createDataFrame(
			Seq(("a", 1), ("a", 1), ("a", 3), ("b", 1), ("b", 3))
		).toDF("name", "age")

		df.createOrReplaceTempView("t1")

		val result = spark.sql("select name, myAverage(age) as age from t1 group by name")

		result.show()

	}


	test("window funnel") {

		val spark = SparkSession.builder().master("local[3]")
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.getOrCreate()

		spark.udf.register("windowFunnel", functions.udaf(WindowFunnel))

		val df = spark.createDataFrame(
			Seq((1, "浏览","1"),
				(1, "点击", "2"),
				(1, "浏览","3"),
				(2, "浏览", "1"),
				(1, "下单", "4")
		)
		).toDF("uid", "event_id", "event_time")

		df.createOrReplaceTempView("t1")

		val result = spark.sql("select uid, " +
			"windowFunnel(event_id, event_time, '浏览,点击,下单', 10) as window_level " +
			"from t1 group by uid")

		result.show()
	}

}
