// scalastyle:off

package org.apache.spark.sql.aqe

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

import scala.util.Random

case class EntryKV(key: Int, value: String)

class AQESuite extends SparkFunSuite {

	test("shuffle partition") {

		val spark = SparkSession.builder()
			.appName("shuffle partitions coalesce").master("local[3]")
			.config("spark.driver.host", "localhost")
			.config("spark.sql.adaptive.enabled", "true")
			.config("spark.sql.adaptive.logLevel", "TRACE")
			.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
			.config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "2")
			// Initial number of shuffle partitions
			// I put a very big number to make the coalescing happen
			// coalesce - about setting the right number of reducer task
			.config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "100")
			.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "30KB")
			// Make it small enough to produce shuffle partitions
			.config("spark.sql.adaptive.localShuffleReader.enabled", "false")
			.config("spark.sql.autoBroadcastJoinThreshold", "1KB")
			.getOrCreate()

		var finalPlanCnt = 0
		val listener = new SparkListener {
			override def onOtherEvent(event: SparkListenerEvent): Unit = {
				event match {
					case SparkListenerSQLAdaptiveExecutionUpdate(_, _, sparkPlanInfo) =>
						if (sparkPlanInfo.simpleString.startsWith(
							"AdaptiveSparkPlan isFinalPlan=true")) {
							finalPlanCnt += 1
						}
					case _ => // ignore other events
				}
			}
		}
		spark.sparkContext.addSparkListener(listener)


		import spark.implicits._

		val input4 = (0 to 50000).toDF("id4").repartition(10)
		val input5 = (0 to 3000).toDF("id5")
		val df = input4.join(input5,
			input4("id4") === input5("id5"), "inner"
		)

		df.collect()

		df.explain(true)

		spark.sparkContext.listenerBus.waitUntilEmpty()

		println(s"finalPlanCnt:$finalPlanCnt")

		spark.sparkContext.removeSparkListener(listener)

		TimeUnit.DAYS.sleep(1)

		spark.stop()
	}


	test("change join strategy") {
		val spark = SparkSession.builder()
			.appName("local shuffle reader").master("local[3]")
			.config("spark.driver.host", "localhost")
			.config("spark.sql.adaptive.enabled", "true")
			.config("spark.sql.adaptive.logLevel", "INFO")
			.config("spark.sql.adaptive.coalescePartitions.enabled", "false")
			.config("spark.sql.adaptive.localShuffleReader.enabled", "false") // true local shuffle reader
			.config("spark.sql.autoBroadcastJoinThreshold", "100B")
			.config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", 0.00000001d)
			.config("spark.sql.shuffle.partitions", 10)
			.getOrCreate()

		import spark.implicits._

		val input4 = spark.sparkContext.parallelize((1 to 200).map(x => (x, x.toString))).toDF("key", "value")
		val input5 = spark.sparkContext.parallelize(Seq(
			(1, "1"), (1, "2"),
			(2, "1"), (2, "2"),
			(3, "1"), (3, "2")
		)).toDF("key", "value")

		input4.createOrReplaceTempView("input4")
		input5.createOrReplaceTempView("input5")

		val sql =
			s"""
				 |select * from input4 join input5 on input4.key = input5.key where input4.value < '1'
			 """.stripMargin
		val df = spark.sql(sql)
		val result = df.collect()
		println(result.size)

		df.explain(true)

		TimeUnit.DAYS.sleep(1)

		spark.stop()

	}

	test("optimize skew join") {
		val spark = SparkSession.builder()
			.appName("optimize skew join").master("local[3]")
			.config("spark.driver.host", "localhost")
			.config("spark.sql.adaptive.enabled", "true")
			.config("spark.sql.adaptive.logLevel", "INFO")
			.config("spark.sql.adaptive.coalescePartitions.enabled", "false")
			.config("spark.sql.adaptive.localShuffleReader.enabled", "false")
  		.config("spark.sql.adaptive.skewJoin.enabled", "true")
  		.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1")
  		.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "10k")
			.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "30KB")
			.config("spark.sql.autoBroadcastJoinThreshold", "1k")
			.config("spark.sql.shuffle.partitions", 10)
			.getOrCreate()

		import spark.implicits._

		val input4 = spark.sparkContext.parallelize(
			(1 to 20000).map(x => {
				if(x % 2 == 0){
					(1, x.toString)
				} else {
					(x, x.toString)
				}
			})
		).toDF("key", "value")

		// input4.groupBy("key").count().show()

		val input5 = spark.sparkContext.parallelize(
			(1 to 200).map(x => (x, x.toString))
		).toDF("key", "value")

		input4.createOrReplaceTempView("input4")
		input5.createOrReplaceTempView("input5")

		val sql =
			s"""
				 |select * from input4 left join input5 on input4.key = input5.key
			 """.stripMargin

		val df = spark.sql(sql)
		val result = df.collect()

		println(result.length)

		df.explain(true)

		TimeUnit.DAYS.sleep(1)

		spark.stop()

	}

}
