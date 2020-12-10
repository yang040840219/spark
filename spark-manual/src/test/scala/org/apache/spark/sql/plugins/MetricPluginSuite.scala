// scalastyle:off
package org.apache.spark.sql.plugins

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession

class MetricPluginSuite extends SparkFunSuite {

	val metricFilePath = getClass.getClassLoader.getResource("metrics.properties").getFile

	val spark = SparkSession.builder()
		.master("local[3]")
		.config("spark.plugins", classOf[MetricPlugin].getName)
		.config("spark.metrics.conf", metricFilePath)
		.getOrCreate()

	test("name") {
		println(MetricPlugin.getClass.getName)
		println(classOf[MetricPlugin].getName)
	}


	test("executor metric") {
		import spark.implicits._
		val df = spark.range(500).repartition(3)
		val incrementDF = df.mapPartitions(iterator => {
			var eventCount = 0
			val incrIterator = iterator.toList.map(value => {
				if(value % 2 == 0){
					 eventCount = eventCount + 1
				}
				value
			}).toIterator
			MetricPlugin.counter.inc(eventCount)
			incrIterator
		})
		incrementDF.count()

		TimeUnit.DAYS.sleep(1)

	}
}
