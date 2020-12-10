// scalastyle:off
package org.apache.spark.sql.catalog

import java.util.Properties

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession

class DMLSuite extends SparkFunSuite {

	val warehouseLocation = "/opt/data/warehouse"

	test("delete") {
		val spark = SparkSession.builder().master("local[2]")
			.config("spark.sql.sources.useV1SourceList", "") // use datasource v2
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.getOrCreate()

		val properties = new Properties()
		properties.setProperty("user", "root")
		properties.setProperty("password", "123456")
		properties.setProperty("driver", "com.mysql.jdbc.Driver")
		val df = spark.read.jdbc("jdbc:mysql://localhost:3306/work", "t_model", properties)
		df.show()
		df.explain(true)
	}

}
