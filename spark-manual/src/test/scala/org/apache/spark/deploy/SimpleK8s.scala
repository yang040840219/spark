// scalastyle:off
package org.apache.spark.deploy


import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

class SimpleK8s extends SparkFunSuite {
  
  
  test("spark submit") {
	val args = Seq("--deploy-mode", "client",
	  "--properties-file", "/opt/data/spark/minikube/spark.properties",
	  "--class", "k8s.WordCount",
	  "--verbose",
	  "spark-internal")
	val appArgs = new SparkSubmitArguments(args)
	val submit = new SparkSubmit()
	val(childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)
	
	println(mainClass)
	println(childArgs)
	println(conf.get("spark.master"))
	
  }
  
  
  test("exception exit") {
	val spark = SparkSession.builder().master("local[2]").getOrCreate()
	import spark.implicits._
	val df = (0 until 100000).toDF("id").selectExpr("id % 5 as key", "id%10 as value")
	  .groupBy("key").agg(count("value1").as("cnt"))
	  .repartition(1).mapPartitions(iter => {
	  iter.map(row => {
		(row.getAs[Int]("key"), row.getAs[Long]("cnt"))
	  })
	}).toDF("key", "value")
	df.show()
	spark.stop()
  }
  
}
