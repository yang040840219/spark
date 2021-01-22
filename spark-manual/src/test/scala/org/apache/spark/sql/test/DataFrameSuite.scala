// scalastyle:off
package org.apache.spark.sql.test

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, Log, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions.{col, countDistinct, udf}
import org.apache.spark.sql.internal.SQLConf

class DataFrameSuite extends AnyFunSuite with Log {

	lazy val spark = SparkSession.builder()
		.master("local[2]").appName("dataframe suite").getOrCreate()

	val json =
		s"""
			 |{
			 |  "name": "a",
			 |  "age":10,
			 |  "id":2,
			 |  "cars": [{"name": "ford", "models":["Focus"]},
			 |           {"name": "bmw", "models":["320", "x5"]}],
			 |  "bike": {
			 |    "name": "Bajaj",
			 |    "models": ["Dominor", "Pulsar"]
			 |  }
			 |}
			 """.stripMargin

	test("flatten json") {

		import spark.implicits._

		val rdd = spark.sparkContext.parallelize(json:: Nil)

		val ds = spark.createDataset(rdd)(Encoders.STRING)

		val df = spark.read.json(ds)

		df.printSchema()

		df.show(truncate = false)

		def flattenDataFrame(df: DataFrame): DataFrame = {
			val fields = df.schema.fields
			val fieldNames = fields.map(x => x.name)
			val length = fields.length
			for(i <- fields.indices) {
				val field = fields(i)
				val fieldName = field.name
				val fieldType = field.dataType
				fieldType match {
					case arrayType: ArrayType => {
						val fieldNamesExcludingArray = fieldNames.filter(_!= fieldName)
						val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
						val explodeDF = df.selectExpr(fieldNamesAndExplode:_*)
						return flattenDataFrame(explodeDF)
					}
					case structType: StructType => {
						val childFieldNames = structType.fieldNames.map(childName => s"$fieldName.$childName")
						val newFieldNames = fieldNames.filter(_!= fieldName) ++ childFieldNames
						val renameCols = newFieldNames.map(name => {
							col(name).as(name.replace(".", "_"))
						}).toSeq
						val explodDF = df.select(renameCols:_*)
						return flattenDataFrame(explodDF)
					}
					case _ =>
				}
			}
			df
		}

		val flatDF = flattenDataFrame(df)
		flatDF.show()
	}


	test("map to struct") {
		val df = spark.createDataFrame(Seq(("a", "b"))).toDF("id", "name")
		df.createOrReplaceTempView("t1")

		val f = (x:String) => "x" -> x

		spark.udf.register("m", udf(f))

		val result = spark.sql("select struct(id, name) as data from t1").select("data.*")
		result.printSchema()
		result.show()
	}

	test("from_json") {

		val df = spark.sql("select array(1,2,4,3)")
		df.printSchema()
	}


	test("distinct") {

		val path = "file:///opt/data/delta/json/realtime"

		val df = spark.read.json(path)

		val cnt = df.groupBy("p_day").agg(countDistinct(col("outroomid")).as("cnt_key"),
			countDistinct(col("localaccountid")).as("cnt_value")
		)

		cnt.show()

		cnt.explain(true)

		TimeUnit.DAYS.sleep(1)
	}



}
