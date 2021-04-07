// scalastyle:off
package org.apache.spark.sql.test

import java.util.Date

import com.JsonUtil
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, Log, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

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
	   |  },
	   |  "timestamp": "2020-12-01"
	   |}
	""".stripMargin
  
  
  test("schema json") {
	import spark.implicits._
	val df = Seq(json).toDF("message")
	df.show()
	val jsonDF = df.map(row => {
	  val value = row.getString(0)
	  Map("a" -> "1", "b" -> "2")
	}).toDF("data")
	
	jsonDF.printSchema()
	jsonDF.select(explode(col("data"))).show()
  }
  
  test("dataframe json") {
	import spark.implicits._
	def jsonToDataFrame(json: String): DataFrame = {
	  val reader = spark.read
	  reader.json(Seq(json).toDS())
	}
	
	val df = jsonToDataFrame(json)
	df.select("bike.*").show()
	df.printSchema()
	df.show()
  }
  
  test("flatten json") {
	
	val rdd = spark.sparkContext.parallelize(json :: Nil)
	val ds = spark.createDataset(rdd)(Encoders.STRING)
	val df = spark.read.json(ds)
	df.printSchema()
	df.show(truncate = false)
	
	def flattenDataFrame(df: DataFrame): DataFrame = {
	  val fields = df.schema.fields
	  val fieldNames = fields.map(x => x.name)
	  val length = fields.length
	  for (i <- fields.indices) {
		val field = fields(i)
		val fieldName = field.name
		val fieldType = field.dataType
		fieldType match {
		  case arrayType: ArrayType => {
			val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
			val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
			val explodeDF = df.selectExpr(fieldNamesAndExplode: _*)
			return flattenDataFrame(explodeDF)
		  }
		  case structType: StructType => {
			val childFieldNames = structType.fieldNames.map(childName => s"$fieldName.$childName")
			val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
			val renameCols = newFieldNames.map(name => {
			  col(name).as(name.replace(".", "_"))
			}).toSeq
			val explodeDF = df.select(renameCols: _*)
			return flattenDataFrame(explodeDF)
		  }
		  case _ =>
		}
	  }
	  df
	}
	
	val flatDF = flattenDataFrame(df.select("bike"))
	flatDF.show()
  }
  
  
  test("row convert to json") {
	val schema = StructType(Seq(StructField("id", IntegerType),
	  StructField("name", StringType),
	  StructField("time", TimestampType)))
	val row = new GenericRowWithSchema(Array(1, "大千世界", new Date()), schema)
	val map = row.getValuesMap[String](schema.fieldNames)
	val time = row.getAs[Any]("time")
	println(time)
	val json = JsonUtil.object2json(map)
	println(json)
  }
  
  test("map to struct") {
	val df = spark.createDataFrame(Seq(("a", "b"))).toDF("id", "name")
	df.createOrReplaceTempView("t1")
	
	val f = (x: String) => "x" -> x
	
	spark.udf.register("m", udf(f))
	
	val result = spark.sql("select struct(id, name) as data from t1").select("data.*")
	result.printSchema()
	result.show()
  }
  
  test("from_json") {
	val df = spark.sql("select array(1,2,4,3)")
	df.printSchema()
	val sparkEnv = SparkEnv.get
  }
  
  test("active") {
	println(SparkSession.active)
  }
  
}
