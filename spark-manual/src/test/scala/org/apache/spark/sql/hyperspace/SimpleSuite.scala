// scalastyle:off
package org.apache.spark.sql.hyperspace

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import com.microsoft.hyperspace._
import com.microsoft.hyperspace.index.IndexConfig


class SimpleSuite extends SparkFunSuite {
  
  val warehouseLocation = "/opt/data/warehouse"
  
  val basePath = "/opt/data/spark"
  
  lazy val spark = SparkSession.builder().master("local[2]")
	.config("spark.sql.warehouse.dir", warehouseLocation)
	.getOrCreate()
  
  test("simple") {
	import spark.implicits._
	val path = s"$basePath/hyperspace/table"
	Seq((1, "name1"), (2, "name2")).toDF("id", "name")
	  .write.mode("overwrite")
	  .parquet(path)
	val df = spark.read.parquet(path)
	val hs = new Hyperspace(spark)
	// create index
	hs.createIndex(df, IndexConfig("idx_id", Seq("id"), includedColumns = Seq("name")))
	val indexes = hs.indexes
	indexes.show(truncate = false)
  }
  
  test("delete index") {
	val hs = new Hyperspace(spark)
	hs.deleteIndex("idx_id")
  }
  
}
