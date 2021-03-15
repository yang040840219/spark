// scalastyle:off
package org.apache.spark.deploy


import org.apache.spark.SparkFunSuite

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
  
}
