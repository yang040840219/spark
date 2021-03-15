// scalastyle:off
package org.apache.spark.sql.test

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession

class SparkSuite extends SparkFunSuite {
  
  test("multi thread spark") {
	
	val spark = SparkSession.builder().master("local[2]").getOrCreate()
	
	val pool = Executors.newFixedThreadPool(10)
	
	for(i <- 0 until 5) {
	  pool.submit(new Runnable {
		override def run(): Unit = {
		  val activeSession = SparkSession.getActiveSession.get
		  val defaultSession = SparkSession.getDefaultSession.get
		  println(s"${Thread.currentThread().getName}, activeSession:${activeSession.hashCode()}, " +
			s"defaultSession:${defaultSession.hashCode()}, sessionState: ${spark.sessionState.hashCode()}")
		}
	  })
	}
	pool.shutdown()
	pool.awaitTermination(5, TimeUnit.MINUTES)
  }
  
}
