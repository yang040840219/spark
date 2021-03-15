// scalastyle:off
package streaming.org.apache.spark.state

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreCoordinatorRef, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class StateSuite extends AnyFunSuite{
  
  private val spark = SparkSession.builder().master("local[2]").getOrCreate()
  private val sessionState = spark.sessionState
  private val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
  
  val checkpointLocation = "/opt/data/spark/streaming/events"
  val operatorId = 1
  val partition = 2
  val queryRunId = UUID.randomUUID()
  
  test("simple state store") {
	val coordRef2 = StateStoreCoordinatorRef.forDriver(spark.sparkContext.env)
	val storeProviderId = StateStoreProviderId(StateStoreId(checkpointLocation, operatorId, partition), queryRunId)
	val keySchema = StructType(Seq(StructField("value", StringType)))
	val valueSchema = StructType(Seq(
	  StructField("groupState", StructType(
		Seq(StructField("numEvents", LongType),
		  StructField("start", LongType),
		  StructField("end", LongType)))),
	  StructField("timeoutTimestamp", LongType)
	))
	val indexOrdinal = None
	val storeVersion = 0
	val storeConf =  new StateStoreConf(sessionState.conf)
 
	val store = StateStore.get(
	  storeProviderId, keySchema, valueSchema, indexOrdinal, storeVersion,
	  storeConf, hadoopConfiguration)
	store.iterator().foreach({
	  case pair => {
		println(pair.key, pair.value)
	  }
	})
  }
  
}
