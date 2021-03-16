// scalastyle:off

package org.apache.spark.sql.test

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Log, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class DPPSuite extends AnyFunSuite with Log {
  
  val warehouseLocation = "/opt/data/warehouse"
  
  val veryBigTable = "very_big_table"
  val smallTable1 = "small_table_1"
  val smallTable2 = "small_table_2"
  
  def generateData(spark: SparkSession) {
	val configs = Map[String, Int](
	  veryBigTable -> 5000,
	  smallTable1 -> 800,
	  smallTable2 -> 20
	)
	
	FileUtils.deleteDirectory(new File(warehouseLocation))
	
	configs.foreach({
	  case (tableName, maxRows) => {
		spark.sql(s"drop table if exists $tableName").collect()
		
		def getPartitionNumber(number: Int): Int = {
		  if (number < 40) {
			5
		  } else if (number < 200) {
			10
		  } else {
			20
		  }
		}
		
		val format = "parquet"
		val nr = s"${tableName}_nr"
		val id = s"${tableName}_id"
		import spark.implicits._
		val inputDF = (1 to maxRows).map(nr => (nr, getPartitionNumber(nr))).toDF(id, nr)
		inputDF.write.partitionBy(nr).format(format).saveAsTable(tableName)
		val analyzeSQL = s"analyze table $tableName compute statistics for columns $id, $nr"
		val df = spark.sql(analyzeSQL)
		df.show()
	  }
	})
  }
  
  test("simple query") {
	val spark = SparkSession.builder()
	  .master("local[2]")
	  .config("hive.exec.dynamic.partition", true)
	  .config("hive.exec.dynamic.partition.mode", "nonstrict")
	  .config("spark.sql.adaptive.enabled", false)
	  .config("spark.sql.warehouse.dir", warehouseLocation)
	  .getOrCreate()
	generateData(spark)
	
	val sql = s"select * from ${veryBigTable} limit 100"
	val df = spark.sql(sql)
	df.rdd.foreachPartition(iterator => {
	  iterator.foreach(row => {
		row.getAs[Int]("id")
	  })
	})
  }
  
  test("without ddp query") {
	val spark = SparkSession.builder()
	  .master("local[2]")
	  .config("hive.exec.dynamic.partition", true)
	  .config("hive.exec.dynamic.partition.mode", "nonstrict")
	  .config("spark.sql.adaptive.enabled", false)
	  .config("spark.sql.warehouse.dir", warehouseLocation)
	  .config(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key, false)
	  .getOrCreate()
	generateData(spark)
	
	val sql =
	  s"""
		 |select
		 |  vb.${veryBigTable}_id, s.${smallTable1}_nr
		 |from ${veryBigTable} vb left join ${smallTable1} s
		 |on vb.${veryBigTable}_nr = s.${smallTable1}_nr
		 |where s.${smallTable1}_id = 5
			 """.stripMargin
	
	val df = spark.sql(sql)
	df.explain(true)
  }
  
  test("ddp query") {
	val spark = SparkSession.builder()
	  .master("local[2]")
	  .config("hive.exec.dynamic.partition", true)
	  .config("hive.exec.dynamic.partition.mode", "nonstrict")
	  .config("spark.sql.adaptive.enabled", false)
	  .config("spark.sql.warehouse.dir", warehouseLocation)
	  .config(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key, true)
	  .getOrCreate()
	generateData(spark)
	
	val sql =
	  s"""
		 |select
		 |  vb.${veryBigTable}_id, s.${smallTable1}_nr
		 |from ${veryBigTable} vb left join ${smallTable1} s
		 |on vb.${veryBigTable}_nr = s.${smallTable1}_nr
		 |where s.${smallTable1}_id = 5
			 """.stripMargin
	
	val df = spark.sql(sql)
	df.explain(true)
  }
  
  
  test("ddp query without reuse exchange") {
	val spark = SparkSession.builder()
	  .master("local[2]")
	  .config("hive.exec.dynamic.partition", true)
	  .config("hive.exec.dynamic.partition.mode", "nonstrict")
	  .config("spark.sql.adaptive.enabled", false)
	  .config("spark.sql.warehouse.dir", warehouseLocation)
	  .config(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key, true)
	  .config(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key, false) // 不仅仅是 BroadcastExchange 才能DPP
	  .config(SQLConf.EXCHANGE_REUSE_ENABLED.key, false)
	  .getOrCreate()
	generateData(spark)
	val sql =
	  s"""
		 |select
		 |  vb.${veryBigTable}_id, s.${smallTable1}_nr
		 |from ${veryBigTable} vb left join ${smallTable1} s
		 |on vb.${veryBigTable}_nr = s.${smallTable1}_nr
		 |where s.${smallTable1}_id = 5
			 """.stripMargin
	
	val df = spark.sql(sql)
	df.explain(true)
  }
  
  
}
