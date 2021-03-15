// scalastyle:off
package org.apache.spark.k8s

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.SparkSubmit

class SimpleK8s extends SparkFunSuite {
  
  
  test("spark submit") {
	val args =
	  s"""
		 |--deploy-mode client --properties-file /opt/data/spark/minikube/spark.properties --class k8s.WordCount spark-internal
	   """.stripMargin
	val method = classOf[SparkSubmit].getMethod("main", classOf[Array[String]])
	method.invoke(null, args)
	TimeUnit.DAYS.sleep(1)
  }
  
}
