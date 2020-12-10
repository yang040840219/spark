// scalastyle:off
package org.apache.spark.sql.catalog

import java.util.Properties

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog

class MultiCatalogSuite extends SparkFunSuite {

	val warehouseLocation = "/opt/data/warehouse"

	test("jdbc v1") {
		val spark = SparkSession.builder().master("local[2]")
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.getOrCreate()

		val properties = new Properties()
		properties.setProperty("user", "root")
		properties.setProperty("password", "123456")
		properties.setProperty("driver", "com.mysql.jdbc.Driver")
		val df = spark.read.jdbc("jdbc:mysql://localhost:3306/work", "t_model", properties)
		df.show()
	}


	test("jdbc v2") {
		// val url = "jdbc:mysql://localhost:3306/work?user=root&password=123456"
		val url = "jdbc:mysql://localhost:3306/work"
		val spark = SparkSession.builder().master("local[2]")
  		.config("spark.sql.planChangeLog.level", "info")
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.config("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
  		.config("spark.sql.catalog.mysql.url", url)
  		.config("spark.sql.catalog.mysql.user", "root")
  		.config("spark.sql.catalog.mysql.password", "123456")
			.getOrCreate()

		val df = spark.read.table("mysql.work.t_model")
		// val sql = "select * from mysql.work.t_model where id <= 12"
		// val sql = "desc mysql.work.t_model"
		// val sql = "delete from mysql.work.t_model where id <= 12"
		// val df = spark.sql(sql)
		// df.explain(true)
		df.show()
	}

	test("multi catalog join") {
		val url = "jdbc:mysql://localhost:3306/work"
		val spark = SparkSession.builder().master("local[2]")
  		.config("spark.sql.catalog.mysql_catalog", "")
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.config("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
			.config("spark.sql.catalog.mysql.url", url)
			.config("spark.sql.catalog.mysql.user", "root")
			.config("spark.sql.catalog.mysql.password", "123456")
			.getOrCreate()

		val df = spark.range(1, 10).toDF("id")
		df.createOrReplaceTempView("t1")

		val sql =
			s"""
				 |select t1.*, t2.* from t1 left join mysql.work.t_model t2 on t1.id = t2.id
			 """.stripMargin

		val joinDF = spark.sql(sql)
		joinDF.show()
	}

}
