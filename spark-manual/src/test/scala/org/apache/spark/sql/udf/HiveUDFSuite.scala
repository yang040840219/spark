// scalastyle:off
package org.apache.spark.sql.udf

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession

class HiveUDFSuite extends SparkFunSuite {
  
  val hiveSite = new Path("/opt/data/server/apache-hive-2.3.7-bin/conf/hive-site.xml")
  
  val spark = SparkSession.builder()
    .config("spark.sql.warehouse.dir", "/opt/data/warehouse")
    .enableHiveSupport()
	.master("local[2]").getOrCreate()
  
  
  test("hive site") {
	spark.sparkContext.hadoopConfiguration.addResource(hiveSite)
	val catalog = spark.sessionState.catalog
	println(catalog)
	catalog.listDatabases().foreach(println)
  }
  
  test("spark addJar") {
	// 不会更新classloader
	spark.sparkContext.addJar("/opt/data/udf/hive-hook-0.0.1.jar")
	
  }
 
  test("print spark") {
	val catalog = spark.catalog
	println(catalog)
  }
  
  test("remove udf") {
	val sql =
	  s"""
		 |drop function custom_array_sort ;
	   """.stripMargin
	spark.sql(sql)
  }
  
  test("show hive udf") {
	val database = "default"
	val funcName = "custom_array_sort_1"
	val function = spark.sessionState.catalog.externalCatalog.getFunction(database, funcName)
	println(function)
  }
  
  test("simple hive udf") {
	// 可以直接读取Hive 中的 UDF 使用
	import spark.implicits._
	val df = Seq((Array("1","3","5","2"))).toDF("numbers")
	df.selectExpr("custom_array_sort_1(numbers, '|') as numbers").show(truncate = false)
	spark.stop()
  }
  
  test("simple udf") {
	val catalog = spark.catalog
	catalog.listFunctions().show(10000, truncate =  false)
	
	spark.sessionState.resourceLoader.addJar("/opt/data/udf/hive-hook-0.0.1.jar")
	
	val sql =
	  s"""
		 |create function if not exists custom_array_sort as "com.customer.udf.ArraySort" ;
	   """.stripMargin
	
	spark.sql(sql)
	import spark.implicits._
	
	val df = Seq((Array("1","3","5","2"))).toDF("numbers")
	
	df.selectExpr("custom_array_sort(numbers, '|') as numbers").show(truncate = false)
 
	catalog.listFunctions().show(10000, truncate =  false)
	
	// TimeUnit.MINUTES.sleep(10)
	spark.stop()
  }
  
  
}
