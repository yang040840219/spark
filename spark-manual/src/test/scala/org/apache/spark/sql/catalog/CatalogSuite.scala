// scalastyle:off
package org.apache.spark.sql.catalog

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog

class CatalogSuite extends SparkFunSuite {

	val warehouseLocation = "/opt/data/warehouse"
	val url = "jdbc:mysql://localhost:3306/work"

	test("spark catalog") {
		val spark = SparkSession.builder().master("local[2]")
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.config("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
			.config("spark.sql.catalog.mysql.url", url)
			.config("spark.sql.catalog.mysql.user", "root")
			.config("spark.sql.catalog.mysql.password", "123456")
			.getOrCreate()
		val catalog = spark.catalog
		println(catalog)
		catalog.listTables().show()
	}


	test("catalog manager") {
		val spark = SparkSession.builder().master("local[2]")
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.config("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
			.config("spark.sql.catalog.mysql.url", url)
			.config("spark.sql.catalog.mysql.user", "root")
			.config("spark.sql.catalog.mysql.password", "123456")
			.getOrCreate()
		val catalogManager = spark.sessionState.catalogManager
		println(catalogManager.currentNamespace.mkString(","))
		val plugin = catalogManager.catalog("mysql")
		println(plugin)
		val tables = plugin.asInstanceOf[JDBCTableCatalog].listTables(Array("work"))
		println(tables.mkString(","))
	}

}
